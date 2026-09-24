package org.dromara.sync.e2e;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.dromara.sync.e2e.Json.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 4: single-table FULL_CDC into Kafka. SeaTunnel writes a private raw Debezium topic;
 * the platform bridge publishes the normalized ENVELOPE events (docs/kafka-event-formats.md) to
 * the operator's topic, keyed by the sync key. Consumes the real output topic and pins the
 * initial-load INSERTs (phase CDC) and the CDC INSERT / UPDATE (with before image) / DELETE events.
 */
@Tag("e2e")
class KafkaTaskE2eTest extends E2eSupport {

    /**
     * Topics created by earlier runs from this checkout. The platform's shared KafkaProducer keeps
     * re-requesting metadata for a topic it published to within the last 5 minutes, and the local
     * broker auto-creates on that request - so an output topic deleted right after its task stops
     * reappears (empty) within seconds. Each run therefore records its topics here and the next
     * run deletes whatever came back. Only names this suite created are ever listed.
     */
    private static final Path PENDING_TOPICS = Path.of("target", "e2e-pending-topics.txt");

    @BeforeAll
    static void deleteResurrectedTopicsOfEarlierRuns() throws Exception {
        if (!Files.exists(PENDING_TOPICS)) return;
        List<String> pending = Files.readAllLines(PENDING_TOPICS).stream().filter(line -> !line.isBlank()).distinct().toList();
        List<String> reappeared = deleteTopics(pending);
        // A topic deleted again now may still come back once more if its last publish was under
        // 5 minutes ago; keep it listed until a later run finds it gone.
        if (reappeared.isEmpty()) Files.delete(PENDING_TOPICS);
        else Files.write(PENDING_TOPICS, reappeared);
        log("earlier runs' topics that had reappeared and were deleted: " + reappeared);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void fullCdcToKafkaPublishesNormalizedEnvelopeEvents() throws Exception {
        String table = table("kfk");
        String topic = table("topic");
        createSourceTable(table, 3);
        AtomicLong taskIdHolder = new AtomicLong();
        // The raw topic is named after the task id, known only once the task exists.
        cleanup.add("delete Kafka topics of " + topic, () -> {
            List<String> topics = taskIdHolder.get() == 0 ? List.of(topic) : List.of(topic, "__ds_raw_" + taskIdHolder.get() + "_v1");
            deleteTopics(topics);
            Files.createDirectories(PENDING_TOPICS.getParent());
            Files.write(PENDING_TOPICS, topics, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        });

        JsonNode created = platform.post("/sync/data-source/" + kafkaTargetId + "/kafka/topics",
            Map.of("topic", topic, "partitions", 1, "replicationFactor", 1));
        assertEquals(topic, text(created, "topic"), "topic create: " + created);

        Map<String, Object> body = taskBody(displayName("kafka"), table, kafkaTargetId, null, topic, "FULL_CDC");
        body.put("kafkaOutputFormat", "ENVELOPE");
        long taskId = createTask(body);
        taskIdHolder.set(taskId);

        String jobId = text(startTask(taskId), "engineJobId");
        awaitTaskStatus(taskId, "RUNNING", JOB_START_TIMEOUT);

        try (TopicReader reader = new TopicReader(topic)) {
            List<Event> snapshot = reader.await("3 snapshot INSERT events", events ->
                Set.of(1L, 2L, 3L).stream().allMatch(id -> events.stream().anyMatch(e -> e.is("INSERT", id))), taskId);
            for (long id = 1; id <= 3; id++) {
                Event event = first(snapshot, "INSERT", id);
                // CDC by contract (docs/kafka-event-formats.md, "phase 的真实含义"): SeaTunnel writes
                // a MySQL-CDC initial-load row as op=c, identical field for field to a binlog
                // INSERT, so the platform has no signal to label it SNAPSHOT and does not guess.
                assertEquals("CDC", text(event.value(), "phase"), "initial-load event phase: " + event);
                assertEquals(defaultName(id), text(event.value().path("data"), "name"), "snapshot row data: " + event);
                assertEquals("source_db", text(event.value().path("source"), "database"));
                assertEquals(table, text(event.value().path("source"), "table"));
            }
            log("snapshot events consumed");

            Db.SOURCE.exec(
                "INSERT INTO " + table + " (" + COLUMNS + ") VALUES " + values(4, "kafka-insert"),
                "UPDATE " + table + " SET name = 'kafka-update' WHERE id = 1",
                "DELETE FROM " + table + " WHERE id = 2");
            List<Event> all = reader.await("CDC INSERT/UPDATE/DELETE events", events ->
                events.stream().anyMatch(e -> e.is("INSERT", 4)) && events.stream().anyMatch(e -> e.is("UPDATE", 1))
                    && events.stream().anyMatch(e -> e.is("DELETE", 2)), taskId);

            Event insert = first(all, "INSERT", 4);
            assertEquals("CDC", text(insert.value(), "phase"));
            assertEquals("kafka-insert", text(insert.value().path("data"), "name"));
            assertEquals(1, insert.key().size(), "the message key is exactly the sync key: " + insert.key());
            assertEquals(insert.key(), insert.value().path("key"), "envelope key equals the message key");

            Event update = first(all, "UPDATE", 1);
            assertEquals("CDC", text(update.value(), "phase"));
            assertEquals("kafka-update", text(update.value().path("data"), "name"), "UPDATE carries the after image: " + update);
            assertEquals(defaultName(1), text(update.value().path("before"), "name"), "UPDATE carries the before image: " + update);

            Event delete = first(all, "DELETE", 2);
            assertEquals("row-2", text(delete.value().path("data"), "name"), "DELETE carries the deleted row: " + delete);
            assertTrue(all.stream().noneMatch(e -> e.is("INSERT", 2) && e.offset() > delete.offset()),
                "nothing may resurrect the deleted row");
            assertTrue(all.stream().noneMatch(e -> "SNAPSHOT".equals(text(e.value(), "phase"))),
                "a FULL_CDC task never publishes SNAPSHOT: " + all);
            log("CDC events consumed");
        }

        JsonNode stopped = platform.post("/sync/task/" + taskId + "/stop", null);
        assertEquals("STOPPED", text(stopped, "status"));
        awaitEngineStatus(jobId, Set.of("CANCELED"), STATE_TIMEOUT);
        assertNotRunningOnEngine(jobId, "ds-task-" + taskId);
        platform.delete("/sync/task/" + taskId);
        assertTrue(Json.isAbsent(task(taskId)));
    }

    /** One consumed output event: key is the sync key as JSON, value the envelope. */
    record Event(long offset, JsonNode key, JsonNode value) {
        boolean is(String op, long id) {
            return op.equals(text(value, "op")) && String.valueOf(id).equals(text(key, "id"));
        }
    }

    private static Event first(List<Event> events, String op, long id) {
        return events.stream().filter(e -> e.is(op, id)).findFirst()
            .orElseThrow(() -> new AssertionError("no " + op + " event for id " + id + " in " + events));
    }

    /** Reads the whole output topic from the beginning with a group-less assigned consumer. */
    private static final class TopicReader implements AutoCloseable {
        private final KafkaConsumer<String, String> consumer;
        private final List<Event> events = new ArrayList<>();

        TopicReader(String topic) {
            consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, E2eConfig.KAFKA_BOOTSTRAP,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
        }

        List<Event> await(String what, Predicate<List<Event>> done, long taskId) {
            try {
                return Await.until(what, DATA_TIMEOUT, this::drain, done, null,
                    list -> list.size() + " events: " + list.stream().map(e -> text(e.value(), "op") + "#" + text(e.key(), "id")).toList());
            } catch (AssertionError ex) {
                throw new AssertionError(ex.getMessage() + "\n  context: " + describeTask(taskId), ex);
            }
        }

        private List<Event> drain() {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                events.add(new Event(record.offset(), Json.parse(record.key()), Json.parse(record.value())));
            }
            return List.copyOf(events);
        }

        @Override
        public void close() {
            consumer.close(Duration.ofSeconds(5));
        }
    }

    /** Deletes those of {@code topics} that exist; returns them. */
    private static List<String> deleteTopics(List<String> topics) throws InterruptedException {
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, E2eConfig.KAFKA_BOOTSTRAP))) {
            Set<String> existing = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            List<String> ours = topics.stream().filter(existing::contains).toList();
            if (!ours.isEmpty()) admin.deleteTopics(ours).all().get(30, TimeUnit.SECONDS);
            return ours;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof UnknownTopicOrPartitionException) return List.of();
            throw new AssertionError("delete topics failed: " + ex.getCause(), ex);
        } catch (TimeoutException ex) {
            throw new AssertionError("delete topics timed out", ex);
        }
    }
}
