package org.dromara.sync.e2e;

import tools.jackson.databind.JsonNode;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The platform backend as an operator's browser sees it: captcha login, then RuoYi {@code R<T>}
 * calls with a bearer token. Never talks to the platform database.
 */
final class PlatformClient {

    /** How long a refused connection (backend restarting) is waited out before giving up. */
    private static final Duration BACKEND_RESTART_GRACE = Duration.ofMinutes(3);
    /** start / resume validate against three databases and submit to a possibly cold engine. */
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(90);

    private final HttpClient http = HttpClient.newBuilder()
        // The dev shell exports http_proxy; loopback traffic must never go through it.
        .proxy(HttpClient.Builder.NO_PROXY)
        .connectTimeout(Duration.ofSeconds(5))
        .build();
    private volatile String token;

    /** Outcome of one call. {@code code} is RuoYi's business code (200 = success), not the HTTP status. */
    record Result(int code, String msg, JsonNode data, String method, String path) {
        boolean ok() {
            return code == 200;
        }

        JsonNode require() {
            if (!ok()) throw new AssertionError(method + " " + path + " failed: code=" + code + ", msg=" + msg);
            return data;
        }
    }

    JsonNode get(String path) {
        return call("GET", path, null).require();
    }

    JsonNode post(String path, Object body) {
        return call("POST", path, body).require();
    }

    JsonNode put(String path, Object body) {
        return call("PUT", path, body).require();
    }

    JsonNode delete(String path) {
        return call("DELETE", path, null).require();
    }

    /** Never throws on a business error; tests assert rejections through {@link Result#code()} / {@link Result#msg()}. */
    Result call(String method, String path, Object body) {
        if (token == null) login();
        Result result = send(method, path, body, true);
        if (result.code() == 401) {
            login();
            result = send(method, path, body, true);
        }
        return result;
    }

    private Result send(String method, String path, Object body, boolean authenticated) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(E2eConfig.BACKEND + path))
            .timeout(REQUEST_TIMEOUT)
            .header("Content-Type", "application/json")
            .header("clientid", E2eConfig.CLIENT_ID);
        if (authenticated) builder.header("Authorization", "Bearer " + token);
        HttpRequest.BodyPublisher publisher = body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(Json.write(body), StandardCharsets.UTF_8);
        HttpRequest request = builder.method(method, publisher).build();
        HttpResponse<String> response = sendWithRestartGrace(request);
        JsonNode root;
        try {
            root = Json.parse(response.body());
        } catch (RuntimeException ex) {
            throw new AssertionError(method + " " + path + " returned HTTP " + response.statusCode()
                + " with a non-JSON body: " + abbreviate(response.body()), ex);
        }
        int code = root.path("code").asInt(response.statusCode());
        return new Result(code, Json.text(root, "msg"), root.get("data"), method, path);
    }

    /**
     * Only a refused connection is retried: the request provably never reached the backend, so
     * even a non-idempotent start cannot be applied twice. Another engineer may redeploy the
     * backend mid-run; this rides that out instead of failing every scenario at once.
     */
    private HttpResponse<String> sendWithRestartGrace(HttpRequest request) {
        long deadline = System.nanoTime() + BACKEND_RESTART_GRACE.toNanos();
        while (true) {
            try {
                return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            } catch (ConnectException ex) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("Backend " + E2eConfig.BACKEND + " refused connections for "
                        + BACKEND_RESTART_GRACE.toSeconds() + "s - is the local stack up (dev.ps1 up -Poc)?", ex);
                }
                sleep(Duration.ofSeconds(3));
            } catch (IOException ex) {
                throw new AssertionError(request.method() + " " + request.uri() + " failed: " + ex, ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted", ex);
            }
        }
    }

    /** Captcha answer comes straight out of Redis - exactly what the login page would have shown. */
    synchronized void login() {
        AssertionError last = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            Result captcha = send("GET", "/auth/code", null, false);
            JsonNode data = captcha.require();
            String uuid = Json.text(data, "uuid");
            String answer = "";
            if (Json.bool(data, "captchaEnabled")) {
                String stored = redisGet("global:captcha_codes:" + uuid);
                if (stored == null) {
                    last = new AssertionError("captcha " + uuid + " not found in Redis");
                    continue;
                }
                answer = stored.replace("\"", "").trim();
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("clientId", E2eConfig.CLIENT_ID);
            body.put("grantType", "password");
            body.put("tenantId", E2eConfig.TENANT_ID);
            body.put("username", E2eConfig.USERNAME);
            body.put("password", E2eConfig.PASSWORD);
            body.put("code", answer);
            body.put("uuid", uuid);
            Result login = send("POST", "/auth/login", body, false);
            String accessToken = login.ok() ? Json.text(login.data(), "access_token") : null;
            if (accessToken != null) {
                token = accessToken;
                return;
            }
            last = new AssertionError("login failed: code=" + login.code() + ", msg=" + login.msg());
        }
        throw last;
    }

    /** Minimal RESP client (AUTH + GET) so the suite needs no Redis configuration of its own. */
    private static String redisGet(String key) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(E2eConfig.REDIS_HOST, E2eConfig.REDIS_PORT), 5000);
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            InputStream in = new BufferedInputStream(socket.getInputStream());
            if (!E2eConfig.REDIS_PASSWORD.isBlank()) {
                writeCommand(out, "AUTH", E2eConfig.REDIS_PASSWORD);
                readReply(in);
            }
            writeCommand(out, "GET", key);
            return readReply(in);
        } catch (IOException ex) {
            throw new AssertionError("Redis " + E2eConfig.REDIS_HOST + ":" + E2eConfig.REDIS_PORT
                + " unreachable while reading the login captcha: " + ex, ex);
        }
    }

    private static void writeCommand(OutputStream out, String... parts) throws IOException {
        StringBuilder command = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            command.append('$').append(part.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(part).append("\r\n");
        }
        out.write(command.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static String readReply(InputStream in) throws IOException {
        int type = in.read();
        String line = readLine(in);
        return switch (type) {
            case '+', ':' -> line;
            case '-' -> throw new IOException("Redis error: " + line);
            case '$' -> {
                int length = Integer.parseInt(line);
                if (length < 0) yield null;
                byte[] payload = in.readNBytes(length);
                readLine(in);
                yield new String(payload, StandardCharsets.UTF_8);
            }
            default -> throw new IOException("unexpected Redis reply type " + (char) type);
        };
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int previous = -1;
        int current;
        while ((current = in.read()) != -1) {
            if (previous == '\r' && current == '\n') break;
            if (previous != -1) line.write(previous);
            previous = current;
        }
        return line.toString(StandardCharsets.UTF_8);
    }

    static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", ex);
        }
    }

    private static String abbreviate(String text) {
        if (text == null) return "null";
        return text.length() <= 300 ? text : text.substring(0, 300) + "...";
    }
}
