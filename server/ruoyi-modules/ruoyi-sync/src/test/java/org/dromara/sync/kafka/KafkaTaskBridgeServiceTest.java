package org.dromara.sync.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Capacity of the bridge worker pool, with real worker threads; only the broker is replaced
 * (the topic precheck is counted, consumers are {@link MockConsumer}s).
 */
@Tag("dev")
class KafkaTaskBridgeServiceTest {

    private final AtomicInteger topicChecks = new AtomicInteger();
    /** Owners whose consumer fails on its first poll, i.e. whose worker dies right away. */
    private final Set<Long> dyingOwners = ConcurrentHashMap.newKeySet();
    private KafkaTaskBridgeService bridge;

    @AfterEach
    void closeBridge() {
        if (bridge != null) bridge.close();
    }

    @Test
    void anOperatorStartIsRefusedWhenThePoolIsFullBeforeAnyTopicCheck() {
        bridge = bridge(2);
        bridge.start(task(1), kafka(), "db");
        bridge.startGroupItem(task(2), kafka(), "db");

        String refused = assertThrows(ServiceException.class, () -> bridge.start(task(3), kafka(), "db")).getMessage();

        assertTrue(refused.startsWith("Kafka 桥接容量已满（2/2）"), refused);
        assertTrue(refused.contains("sync.kafka-bridge.max-workers"), refused);
        assertEquals(2, topicChecks.get(), "refused before the AdminClient round trip");
        assertEquals(Set.of(1L, 2L), bridge.localOwnerIds());
        assertTrue(bridge.isRunning(1L) && bridge.isRunning(2L));
        assertFalse(bridge.isRunning(3L));
    }

    @Test
    void aBackgroundStartParksTheOwnerInsteadOfFailingAndStaysCheap() {
        bridge = bridge(1);
        bridge.start(task(1), kafka(), "db");

        assertFalse(bridge.tryStart(task(2), kafka(), "db"));
        assertFalse(bridge.tryStartGroupItem(task(2), kafka(), "db"));

        assertEquals(1, topicChecks.get(), "a parked owner costs no AdminClient round trip");
        assertFalse(bridge.isRunning(2L));
        assertEquals(Set.of(1L, 2L), bridge.localOwnerIds(), "retired like a worker once the owner stops");
        String shown = bridge.decorateLastError(2L, "checkpoint 超时");
        assertTrue(shown.startsWith("Kafka 桥接等待容量：本实例桥接已满（1/1）"), shown);
        assertTrue(shown.contains("sync.kafka-bridge.max-workers") && shown.endsWith("；checkpoint 超时"), shown);
        assertEquals("x", bridge.decorateLastError(1L, "x"));
        assertNull(bridge.decorateLastError(1L, null));
    }

    @Test
    void parkedOwnersGetTheNextFreeSlotBeforeNewOperatorStarts() {
        bridge = bridge(2);
        bridge.start(task(1), kafka(), "db");
        bridge.start(task(2), kafka(), "db");
        assertFalse(bridge.tryStart(task(3), kafka(), "db"));

        bridge.stop(1L);
        String refused = assertThrows(ServiceException.class, () -> bridge.start(task(4), kafka(), "db")).getMessage();
        assertTrue(refused.contains("另有 1 个运行中的任务/表项在排队等待桥接"), refused);

        assertTrue(bridge.tryStart(task(3), kafka(), "db"));
        assertTrue(bridge.isRunning(3L));
        assertEquals("", bridge.decorateLastError(3L, ""), "the notice goes away with the parking");
    }

    @Test
    void stoppingAParkedOwnerReleasesItsReservation() {
        bridge = bridge(1);
        bridge.start(task(1), kafka(), "db");
        assertFalse(bridge.tryStart(task(2), kafka(), "db"));

        bridge.stop(2L);
        bridge.stop(1L);

        assertEquals(Set.of(), bridge.localOwnerIds());
        bridge.start(task(5), kafka(), "db");
        assertTrue(bridge.isRunning(5L));
    }

    @Test
    void anOwnerWhoseWorkerDiedKeepsItsSlotForTheRestart() throws Exception {
        bridge = bridge(1);
        dyingOwners.add(9L);
        bridge.start(task(9), kafka(), "db");
        awaitNotRunning(9L);

        // The dead worker still holds the only slot, so no one else may take it...
        assertThrows(ServiceException.class, () -> bridge.start(task(10), kafka(), "db"));
        // ...and the heal of its own owner needs no free one.
        dyingOwners.clear();
        assertTrue(bridge.tryStart(task(9), kafka(), "db"));
        assertTrue(bridge.isRunning(9L));
    }

    @Test
    void aGroupResumeIsCheckedAsAWhole() {
        bridge = bridge(3);
        bridge.start(task(1), kafka(), "db");

        bridge.requireCapacity(List.of(1L, 2L, 3L)); // 1 already has a worker: needs 2 of the 2 free

        String refused = assertThrows(ServiceException.class, () -> bridge.requireCapacity(List.of(2L, 3L, 4L))).getMessage();
        assertTrue(refused.startsWith("Kafka 桥接容量不足（已用 1/3，本次需要 3 个）"), refused);
    }

    private KafkaTaskBridgeService bridge(int maxWorkers) {
        KafkaBridgeProperties properties = new KafkaBridgeProperties();
        properties.setMaxWorkers(maxWorkers);
        return new KafkaTaskBridgeService(mock(KafkaEventNormalizer.class), mock(KafkaEventProducer.class),
            JsonMapper.builder().build(), properties) {

            @Override
            void ensureTopics(SyncTask task, DataSource target) {
                topicChecks.incrementAndGet();
            }

            @Override
            Consumer<String, String> openConsumer(Properties consumerProperties) {
                String group = consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
                return new IdleConsumer(dyingOwners.stream().anyMatch(id -> group.equals("ds-task-" + id + "-bridge")));
            }
        };
    }

    private void awaitNotRunning(long ownerId) throws InterruptedException {
        for (int i = 0; i < 500 && bridge.isRunning(ownerId); i++) Thread.sleep(10);
        assertFalse(bridge.isRunning(ownerId));
    }

    private static SyncTask task(long id) {
        SyncTask task = new SyncTask();
        task.setTaskId(id);
        task.setSourceTable("customers");
        task.setTargetTable("customer-events");
        task.setSelectedColumns("id,name");
        task.setSyncKeyColumns("id");
        task.setConfigVersion(1);
        return task;
    }

    private static DataSource kafka() {
        DataSource kafka = new DataSource();
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka.example");
        kafka.setPort(9092);
        return kafka;
    }

    /** Polls nothing, slowly enough not to spin; fails on the first poll when told to. */
    private static final class IdleConsumer extends MockConsumer<String, String> {
        private final boolean dies;

        private IdleConsumer(boolean dies) {
            super(OffsetResetStrategy.EARLIEST);
            this.dies = dies;
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration timeout) {
            if (dies) throw new KafkaException("broker connection lost");
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return super.poll(timeout);
        }
    }
}
