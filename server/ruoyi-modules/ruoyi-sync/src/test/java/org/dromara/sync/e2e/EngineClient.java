package org.dromara.sync.e2e;

import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-mostly view of the SeaTunnel Zeta REST API, used to check what the platform claims
 * against what the engine is actually doing. {@link #stopJob} exists only for cleanup of jobs
 * this suite started.
 */
final class EngineClient {

    private final HttpClient http = HttpClient.newBuilder()
        .proxy(HttpClient.Builder.NO_PROXY)
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    /** {@code /job-info/{id}}; an empty object when the engine does not know the job. */
    JsonNode jobInfo(String jobId) {
        return Json.parse(send(HttpRequest.newBuilder(URI.create(E2eConfig.ENGINE + "/job-info/" + jobId)).GET()));
    }

    /** Engine {@code jobStatus}, or a readable placeholder when unknown / unreachable (never throws). */
    String jobStatus(String jobId) {
        if (jobId == null) return "<no job id>";
        try {
            String status = Json.text(jobInfo(jobId), "jobStatus");
            return status == null ? "<unknown to engine>" : status;
        } catch (RuntimeException | AssertionError ex) {
            return "<engine unreachable: " + ex.getMessage() + ">";
        }
    }

    /** Engine error message of a job, if any (never throws). */
    String jobError(String jobId) {
        if (jobId == null) return null;
        try {
            return Json.text(jobInfo(jobId), "errorMsg");
        } catch (RuntimeException | AssertionError ex) {
            return null;
        }
    }

    /** Job id -> job name of everything the engine currently lists as running. */
    Map<String, String> runningJobs() {
        JsonNode body = Json.parse(send(HttpRequest.newBuilder(URI.create(E2eConfig.ENGINE + "/running-jobs")).GET()));
        Map<String, String> jobs = new LinkedHashMap<>();
        for (JsonNode job : body) {
            String id = Json.text(job, "jobId");
            if (id != null) jobs.put(id, Json.text(job, "jobName"));
        }
        return jobs;
    }

    boolean isRunning(String jobId, String jobName) {
        Map<String, String> running = runningJobs();
        return (jobId != null && running.containsKey(jobId)) || (jobName != null && running.containsValue(jobName));
    }

    /** Hard cancel, no savepoint. Cleanup only - and only for a job id / name this suite owns. */
    void stopJob(String jobId) {
        String body = "{\"jobId\":" + Long.parseLong(jobId) + ",\"isStopWithSavePoint\":false}";
        send(HttpRequest.newBuilder(URI.create(E2eConfig.ENGINE + "/stop-job"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)));
    }

    private String send(HttpRequest.Builder builder) {
        try {
            HttpResponse<String> response = http.send(builder.timeout(Duration.ofSeconds(15)).build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() >= 400) {
                throw new AssertionError("SeaTunnel REST HTTP " + response.statusCode() + ": " + response.body());
            }
            return response.body();
        } catch (IOException ex) {
            throw new AssertionError("SeaTunnel REST " + E2eConfig.ENGINE + " unreachable: " + ex, ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", ex);
        }
    }
}
