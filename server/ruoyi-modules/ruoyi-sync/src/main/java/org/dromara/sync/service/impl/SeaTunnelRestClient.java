package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** Small REST adapter for the SeaTunnel Zeta 2.3.13 lifecycle API. */
@Component
@RequiredArgsConstructor
public class SeaTunnelRestClient {

    private final SeaTunnelProperties properties;
    private final JsonMapper jsonMapper;

    public SubmitResult submit(String jobName, String config, String jobId, boolean withSavepoint) {
        URI uri = uri("/submit-job", builder -> {
            builder.queryParam("format", "hocon");
            builder.queryParam("jobName", jobName);
            if (StringUtils.isNotBlank(jobId)) {
                builder.queryParam("jobId", jobId);
                builder.queryParam("isStartWithSavePoint", withSavepoint);
            }
        });
        JsonNode body = request(client -> client.post()
            .uri(uri)
            .contentType(MediaType.TEXT_PLAIN)
            .body(config)
            .retrieve()
            .body(String.class));
        String returnedJobId = text(body, "jobId");
        if (StringUtils.isBlank(returnedJobId)) {
            throw new ServiceException("SeaTunnel 提交成功但未返回 jobId");
        }
        return new SubmitResult(returnedJobId, text(body, "jobName"));
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

    private RestClient client() {
        return RestClient.builder().baseUrl(trimEndpoint(properties.getEndpoint())).build();
    }

    private URI uri(String path, java.util.function.Consumer<UriComponentsBuilder> customizer) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(trimEndpoint(properties.getEndpoint()))
            .path(path);
        customizer.accept(builder);
        return builder.build().encode().toUri();
    }

    private JsonNode request(java.util.function.Function<RestClient, String> operation) {
        try {
            String response = operation.apply(client());
            return jsonMapper.readTree(response == null ? "{}" : response);
        } catch (RestClientResponseException ex) {
            String detail = ex.getResponseBodyAsString();
            throw new ServiceException("SeaTunnel 接口调用失败（HTTP " + ex.getStatusCode().value() + "）：" + safeDetail(detail));
        } catch (ServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ServiceException("SeaTunnel 接口不可用：" + safeDetail(ex.getMessage()));
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
        if (StringUtils.isBlank(detail)) return "无详细信息";
        return detail.replaceAll("(?i)(password\\s*[=:]\\s*)[^,;\\s}]+", "$1******");
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
        static CheckpointSnapshot empty() {
            return new CheckpointSnapshot(null, null, null);
        }
    }
}
