package org.dromara.sync.engine;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.support.SyncText;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Small REST adapter for the SeaTunnel Zeta 2.3.13 lifecycle API. */
@Component
@RequiredArgsConstructor
public class SeaTunnelRestClient {

    private final SeaTunnelProperties properties;
    private final JsonMapper jsonMapper;
    private volatile RestClient cachedClient;

    public SubmitResult submit(String jobName, String config, String jobId, boolean withSavepoint) {
        URI uri = uri("/submit-job", builder -> {
            builder.queryParam("format", "hocon");
            builder.queryParam("jobName", jobName);
            if (StringUtils.isNotBlank(jobId)) {
                builder.queryParam("jobId", jobId);
                builder.queryParam("isStartWithSavePoint", withSavepoint);
            }
        });
        JsonNode body;
        try {
            body = requestRaw(client -> client.post()
                .uri(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .body(config)
                .retrieve()
                .body(String.class));
        } catch (EngineUnreachable unreachable) {
            // We never saw an answer, so we cannot know whether the engine took the job. It
            // usually did - a cold Zeta accepts the submit and only exceeds our request timeout
            // while answering. Leaving it unclaimed is the worst outcome: the job keeps writing
            // to the target while the platform records FAILED with no jobId, never polls it, and
            // a retry would submit a second job onto the same table. Job names are deterministic,
            // so look ours up and adopt it. An HTTP error is NOT routed here on purpose: that is
            // the engine answering "no", and nothing was created.
            String adopted = findRunningJobIdByName(jobName);
            if (adopted == null) throw unreachable.asServiceException();
            return new SubmitResult(adopted, jobName);
        }
        String returnedJobId = text(body, "jobId");
        if (StringUtils.isBlank(returnedJobId)) {
            throw new ServiceException("SeaTunnel 提交成功但未返回 jobId");
        }
        return new SubmitResult(returnedJobId, text(body, "jobName"));
    }

    /**
     * The id of the running job named {@code jobName}, or null when the engine does not list
     * one (yet). A job accepted moments ago may still be compiling and not appear here at all,
     * which is why this is only a fast path - {@code EngineOrphanSweeper} is the safety net.
     */
    public String findRunningJobIdByName(String jobName) {
        return runningJobIdsByName().get(jobName);
    }

    /** Job name -> job id for everything the engine currently runs; empty when it is unreachable. */
    public Map<String, String> runningJobIdsByName() {
        try {
            JsonNode body = request(client -> client.get()
                .uri(uri("/running-jobs", ignored -> { }))
                .retrieve()
                .body(String.class));
            Map<String, String> result = new LinkedHashMap<>();
            for (JsonNode job : body) {
                String name = text(job, "jobName");
                String id = text(job, "jobId");
                if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(id)) result.put(name, id);
            }
            return result;
        } catch (RuntimeException ex) {
            // Nothing to reconcile against while the engine is unreachable.
            return Map.of();
        }
    }

    public JobSnapshot status(String jobId) {
        JsonNode body = request(client -> client.get()
            .uri(uri("/job-info/" + jobId, ignored -> { }))
            .retrieve()
            .body(String.class));
        String returnedJobId = text(body, "jobId");
        if (StringUtils.isBlank(returnedJobId)) {
            throw new ServiceException("SeaTunnel 中不存在作业 " + jobId);
        }
        return new JobSnapshot(returnedJobId, text(body, "jobName"), text(body, "jobStatus"), text(body, "errorMsg"), body.path("metrics"));
    }

    public void stop(String jobId, boolean withSavepoint, boolean force) {
        JsonNode body = jsonMapper.createObjectNode()
            .put("jobId", parseJobId(jobId))
            .put("isStopWithSavePoint", withSavepoint)
            .put("force", force);
        request(client -> client.post()
            .uri(uri("/stop-job", ignored -> { }))
            .contentType(MediaType.APPLICATION_JSON)
            .body(body.toString())
            .retrieve()
            .body(String.class));
    }

    public CheckpointSnapshot checkpoints(String jobId) {
        JsonNode body = request(client -> client.get()
            .uri(uri("/jobs/checkpoints/" + jobId, ignored -> { }))
            .retrieve()
            .body(String.class));
        List<JsonNode> candidates = new ArrayList<>();
        for (JsonNode pipeline : body.path("pipelines")) {
            addIfObject(candidates, pipeline.path("latestCompleted"));
            addIfObject(candidates, pipeline.path("latestSavepoint"));
        }
        JsonNode latest = candidates.stream()
            .filter(node -> node.path("completedTimestamp").canConvertToLong() || node.path("triggerTimestamp").canConvertToLong())
            .max((left, right) -> Long.compare(timestamp(left), timestamp(right)))
            .orElse(null);
        if (latest == null) return CheckpointSnapshot.empty();
        long timestamp = timestamp(latest);
        return new CheckpointSnapshot(
            text(latest, "checkpointId"),
            timestamp == 0 ? null : LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()),
            text(latest, "status"));
    }

    /**
     * RestClient is thread-safe and meant to be shared - rebuilding one per call discards
     * the underlying HTTP connection pool every time. Both connect and read are bounded by
     * {@code sync.engine.request-timeout}: every caller here runs either in a request
     * thread or on the shared scheduling pool, and an unbounded call against a wedged
     * engine would otherwise stall every reconciler behind it. A timeout surfaces as the
     * same ServiceException as any other REST failure, so the status-failure tolerance in
     * the lifecycle services applies unchanged.
     */
    private RestClient client() {
        RestClient client = cachedClient;
        if (client == null) {
            synchronized (this) {
                client = cachedClient;
                if (client == null) {
                    Duration timeout = requestTimeout();
                    JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(
                        HttpClient.newBuilder().connectTimeout(timeout).build());
                    requestFactory.setReadTimeout(timeout);
                    client = RestClient.builder()
                        .baseUrl(trimEndpoint(properties.getEndpoint()))
                        .requestFactory(requestFactory)
                        .build();
                    cachedClient = client;
                }
            }
        }
        return client;
    }

    private Duration requestTimeout() {
        Duration configured = properties.getRequestTimeout();
        return configured == null || configured.isZero() || configured.isNegative() ? Duration.ofSeconds(10) : configured;
    }

    private URI uri(String path, java.util.function.Consumer<UriComponentsBuilder> customizer) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(trimEndpoint(properties.getEndpoint()))
            .path(path);
        customizer.accept(builder);
        return builder.build().encode().toUri();
    }

    /**
     * Every caller but {@link #submit} treats the two failure kinds the same, so this maps
     * {@link EngineUnreachable} straight to the usual ServiceException.
     */
    private JsonNode request(java.util.function.Function<RestClient, String> operation) {
        try {
            return requestRaw(operation);
        } catch (EngineUnreachable ex) {
            throw ex.asServiceException();
        }
    }

    private JsonNode requestRaw(java.util.function.Function<RestClient, String> operation) {
        try {
            String response = operation.apply(client());
            return jsonMapper.readTree(response == null ? "{}" : response);
        } catch (RestClientResponseException ex) {
            // The engine answered, it just answered with an error - the request had no effect.
            String detail = ex.getResponseBodyAsString();
            throw new ServiceException("SeaTunnel 接口调用失败（HTTP " + ex.getStatusCode().value() + "）：" + safeDetail(detail));
        } catch (ServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            // No answer at all (timeout, connection refused, reset): the request may or may not
            // have been applied. Only submit() cares about the difference.
            throw new EngineUnreachable(safeDetail(ex.getMessage()));
        }
    }

    /** No HTTP answer was received, so the effect of the request is unknown. */
    private static final class EngineUnreachable extends RuntimeException {
        private EngineUnreachable(String detail) {
            super(detail);
        }

        private ServiceException asServiceException() {
            return new ServiceException("SeaTunnel 接口不可用：" + getMessage());
        }
    }

    private static long parseJobId(String jobId) {
        try {
            return Long.parseLong(jobId);
        } catch (NumberFormatException ex) {
            throw new ServiceException("SeaTunnel jobId 格式无效");
        }
    }

    private static void addIfObject(List<JsonNode> nodes, JsonNode node) {
        if (node != null && node.isObject()) nodes.add(node);
    }

    private static long timestamp(JsonNode node) {
        JsonNode completed = node.path("completedTimestamp");
        return completed.canConvertToLong() ? completed.asLong() : node.path("triggerTimestamp").asLong(0);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static String safeDetail(String detail) {
        return StringUtils.isBlank(detail) ? "无详细信息" : SyncText.redactSecrets(detail);
    }

    private static String trimEndpoint(String endpoint) {
        if (StringUtils.isBlank(endpoint)) return "http://localhost:18080";
        return endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    }

    public record SubmitResult(String jobId, String jobName) {
    }

    public record JobSnapshot(String jobId, String jobName, String status, String errorMessage, JsonNode metrics) {
    }

    public record CheckpointSnapshot(String id, LocalDateTime time, String status) {
        public static CheckpointSnapshot empty() {
            return new CheckpointSnapshot(null, null, null);
        }
    }
}
