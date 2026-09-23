package org.dromara.sync.engine;

import com.sun.net.httpserver.HttpServer;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.SeaTunnelProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the adapter against a real (tiny) HTTP server, because the behaviour that matters
 * here is exactly what the transport does: a submit whose answer is lost must not leave the
 * accepted job unclaimed, while a submit the engine actively refused must not adopt anything.
 */
@Tag("dev")
class SeaTunnelRestClientTest {

    private HttpServer server;
    private SeaTunnelRestClient client;
    private final List<String> paths = new ArrayList<>();
    private final AtomicInteger submitCalls = new AtomicInteger();

    /** Set per test: what /submit-job does. Returning null means "hang until the read times out". */
    private Supplier<Response> submitBehaviour = () -> new Response(200, "{\"jobId\":\"7\",\"jobName\":\"ds-task-1\"}");
    private String runningJobsBody = "[]";

    private record Response(int status, String body) {
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // Needs real concurrency: the lost-answer test keeps /submit-job occupied while the
        // adapter asks /running-jobs on another connection.
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/submit-job", exchange -> {
            paths.add("/submit-job");
            submitCalls.incrementAndGet();
            Response response = submitBehaviour.get();
            if (response == null) {
                // Never answer: the client's read timeout fires, which is the ambiguous case.
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                exchange.close();
                return;
            }
            respond(exchange, response);
        });
        server.createContext("/running-jobs", exchange -> {
            paths.add("/running-jobs");
            respond(exchange, new Response(200, runningJobsBody));
        });
        server.start();

        SeaTunnelProperties properties = new SeaTunnelProperties();
        properties.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort());
        properties.setRequestTimeout(Duration.ofMillis(600));
        client = new SeaTunnelRestClient(properties, JsonMapper.builder().build());
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void aNormalSubmitReturnsTheEnginesJobId() {
        SeaTunnelRestClient.SubmitResult result = client.submit("ds-task-1", "env {}", null, false);

        assertEquals("7", result.jobId());
        assertEquals(List.of("/submit-job"), paths);
    }

    @Test
    void aSubmitWhoseAnswerIsLostAdoptsTheJobTheEngineAlreadyAccepted() {
        submitBehaviour = () -> null; // times out
        runningJobsBody = "[{\"jobId\":\"991\",\"jobName\":\"ds-task-1\",\"jobStatus\":\"RUNNING\"},"
            + "{\"jobId\":\"992\",\"jobName\":\"ds-task-2\",\"jobStatus\":\"RUNNING\"}]";

        SeaTunnelRestClient.SubmitResult result = client.submit("ds-task-1", "env {}", null, false);

        assertEquals("991", result.jobId(), "the job named ds-task-1, not the other one");
        assertTrue(paths.contains("/running-jobs"), "the adapter must look the job up");
    }

    @Test
    void aLostAnswerWithNoMatchingJobStillFails() {
        submitBehaviour = () -> null;
        runningJobsBody = "[{\"jobId\":\"992\",\"jobName\":\"ds-task-2\",\"jobStatus\":\"RUNNING\"}]";

        ServiceException ex = assertThrows(ServiceException.class, () -> client.submit("ds-task-1", "env {}", null, false));

        assertTrue(ex.getMessage().contains("SeaTunnel 接口不可用"), ex.getMessage());
    }

    @Test
    void anEngineThatAnsweredWithAnErrorIsNeverAdoptedFrom() {
        // A 500 means the engine refused the job, so nothing was created - adopting whatever
        // happens to carry this name would attach the task to an unrelated (older) job.
        submitBehaviour = () -> new Response(500, "{\"message\":\"config invalid\"}");
        runningJobsBody = "[{\"jobId\":\"991\",\"jobName\":\"ds-task-1\",\"jobStatus\":\"RUNNING\"}]";

        ServiceException ex = assertThrows(ServiceException.class, () -> client.submit("ds-task-1", "env {}", null, false));

        assertTrue(ex.getMessage().contains("HTTP 500"), ex.getMessage());
        assertEquals(List.of("/submit-job"), paths, "no lookup must happen");
    }

    @Test
    void aSubmitThatReturnsNoJobIdIsAFailure() {
        submitBehaviour = () -> new Response(200, "{}");

        assertTrue(assertThrows(ServiceException.class, () -> client.submit("ds-task-1", "env {}", null, false))
            .getMessage().contains("未返回 jobId"));
    }

    @Test
    void lookingUpAJobNameOnADeadEngineReturnsNullRatherThanThrowing() {
        server.stop(0);
        assertNull(client.findRunningJobIdByName("ds-task-1"));
        assertTrue(client.runningJobIdsByName().isEmpty());
    }

    @Test
    void runningJobsAreReturnedAsANameToIdMap() {
        runningJobsBody = "[{\"jobId\":\"1\",\"jobName\":\"ds-task-1\"},{\"jobId\":\"2\",\"jobName\":\"ds-task-2\"}]";

        assertEquals(Map.of("ds-task-1", "1", "ds-task-2", "2"), client.runningJobIdsByName());
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, Response response) throws IOException {
        byte[] payload = response.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(response.status(), payload.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(payload);
        }
    }
}
