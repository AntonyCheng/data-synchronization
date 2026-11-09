package org.dromara.sync.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaRawTopic;
import org.dromara.sync.domain.SyncTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Capacity of the bridge worker pool, with real worker threads; only the broker is replaced
 * (the topic precheck is counted, consumers are {@link MockConsumer}s).
 */
@Tag("dev")
class KafkaTaskBridgeServiceTest {

    private final AtomicInteger topicChecks = new AtomicInteger();
    private final KafkaBridgeStateStore stateStore = mock(KafkaBridgeStateStore.class);
    /** Owners whose consumer throws an Error on its first poll, i.e. whose worker dies right away. */
    private final Set<Long> dyingOwners = ConcurrentHashMap.newKeySet();
    /** Owners whose consumer fails (a KafkaException) on every poll. */
    private final Set<Long> failingOwners = ConcurrentHashMap.newKeySet();
    private KafkaTaskBridgeService bridge;

    @AfterEach
    void closeBridge() {
        if (bridge != null) bridge.close();
    }

    @Test
    void anOperatorStartIsRefusedWhenThePoolIsFullBeforeAnyTopicCheck() {
        bridge = bridge(2);
        bridge.start(task(1), kafka(), source(), true);
        bridge.startGroupItem(task(2), kafka(), source(), true);

        String refused = assertThrows(ServiceException.class, () -> bridge.start(task(3), kafka(), source(), true)).getMessage();

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
        bridge.start(task(1), kafka(), source(), true);

        assertFalse(bridge.tryStart(task(2), kafka(), source()));
        assertFalse(bridge.tryStartGroupItem(task(2), kafka(), source()));

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
        bridge.start(task(1), kafka(), source(), true);
        bridge.start(task(2), kafka(), source(), true);
        assertFalse(bridge.tryStart(task(3), kafka(), source()));

        bridge.stop(1L);
        String refused = assertThrows(ServiceException.class, () -> bridge.start(task(4), kafka(), source(), true)).getMessage();
        assertTrue(refused.contains("另有 1 个运行中的任务/表项在排队等待桥接"), refused);

        assertTrue(bridge.tryStart(task(3), kafka(), source()));
        assertTrue(bridge.isRunning(3L));
        assertEquals("", bridge.decorateLastError(3L, ""), "the notice goes away with the parking");
    }

    @Test
    void stoppingAParkedOwnerReleasesItsReservation() {
        bridge = bridge(1);
        bridge.start(task(1), kafka(), source(), true);
        assertFalse(bridge.tryStart(task(2), kafka(), source()));

        bridge.stop(2L);
        bridge.stop(1L);

        assertEquals(Set.of(), bridge.localOwnerIds());
        bridge.start(task(5), kafka(), source(), true);
        assertTrue(bridge.isRunning(5L));
    }

    @Test
    void anOwnerWhoseWorkerDiedKeepsItsSlotForTheRestart() throws Exception {
        bridge = bridge(1);
        dyingOwners.add(9L);
        bridge.start(task(9), kafka(), source(), true);
        awaitNotRunning(9L);

        // The dead worker still holds the only slot, so no one else may take it...
        assertThrows(ServiceException.class, () -> bridge.start(task(10), kafka(), source(), true));
        // ...and the heal of its own owner needs no free one.
        dyingOwners.clear();
        assertTrue(bridge.tryStart(task(9), kafka(), source()));
        assertTrue(bridge.isRunning(9L));
    }

    @Test
    void theBacklogOfAStoppedBridgeCountsTheRawRecordsWaitingBehindIt() {
        LocalDateTime since = LocalDateTime.now().minusMinutes(1);
        // Publishing: the engine's own backlog is all there is.
        assertEquals(2L, KafkaTaskBridgeService.effectiveBacklogRows(2L, null, 5L));
        assertEquals(7L, KafkaTaskBridgeService.effectiveBacklogRows(2L, since, 5L));
        assertEquals(5L, KafkaTaskBridgeService.effectiveBacklogRows(null, since, 5L));
        // Stopped, but Kafka could not say how much waits behind it.
        assertEquals(2L, KafkaTaskBridgeService.effectiveBacklogRows(2L, since, null));
        assertNull(KafkaTaskBridgeService.effectiveBacklogRows(null, null, null));
    }

    @Test
    void aFailingConsumerKeepsItsWorkerRecordsTheStopAndRetries() {
        bridge = bridge(1);
        failingOwners.add(11L);
        bridge.startGroupItem(task(11), kafka(), source(), false);

        // Recorded on the table item row, with the time the stop began, and again on every attempt.
        verify(stateStore, timeout(5000).atLeast(2)).reportBlocked(eq(KafkaRawTopic.OWNER_GROUP_ITEM), eq(11L),
            argThat(reason -> reason.startsWith("Kafka 桥接已暂停发布：KafkaException：broker connection lost")), any(), isNull());
        // Alive between attempts: the reconciler and the status refresh leave it alone, and it holds its slot.
        assertTrue(bridge.isRunning(11L));
        assertThrows(ServiceException.class, () -> bridge.start(task(12), kafka(), source(), true));

        failingOwners.clear();
        // Once by the operator start, once when a healthy session found nothing left to publish.
        verify(stateStore, timeout(5000).times(2)).clearBlocked(KafkaRawTopic.OWNER_GROUP_ITEM, 11L);
    }

    @Test
    void anOperatorStartClearsTheOldStopAndMarksAFreshRun() {
        bridge = bridge(2);
        bridge.start(task(1), kafka(), source(), true);
        bridge.startGroupItem(task(2), kafka(), source(), false);

        verify(stateStore).clearBlocked(KafkaRawTopic.OWNER_TASK, 1L);
        verify(stateStore).allowReset(null, "__ds_raw_1_v1");
        verify(stateStore).clearBlocked(KafkaRawTopic.OWNER_GROUP_ITEM, 2L);
        verify(stateStore, never()).allowReset(null, "__ds_raw_2_v1");
        // A background start (the job already runs) changes neither.
        bridge.stop(1L);
        bridge.tryStart(task(1), kafka(), source());
        verify(stateStore, times(1)).clearBlocked(KafkaRawTopic.OWNER_TASK, 1L);
    }

    @Test
    void aGroupResumeIsCheckedAsAWhole() {
        bridge = bridge(3);
        bridge.start(task(1), kafka(), source(), true);

        bridge.requireCapacity(List.of(1L, 2L, 3L)); // 1 already has a worker: needs 2 of the 2 free

        String refused = assertThrows(ServiceException.class, () -> bridge.requireCapacity(List.of(2L, 3L, 4L))).getMessage();
        assertTrue(refused.startsWith("Kafka 桥接容量不足（已用 1/3，本次需要 3 个）"), refused);
    }

    private KafkaTaskBridgeService bridge(int maxWorkers) {
        KafkaBridgeProperties properties = new KafkaBridgeProperties();
        properties.setMaxWorkers(maxWorkers);
        properties.setRetryInitialBackoff(Duration.ofMillis(50));
        properties.setRetryMaxBackoff(Duration.ofMillis(200));
        return new KafkaTaskBridgeService(mock(KafkaEventNormalizer.class), mock(KafkaEventProducer.class),
            JsonMapper.builder().build(), properties, new SeaTunnelProperties(), stateStore) {

            @Override
            long targetTopicMaxMessageBytes(KafkaConnection connection, String topic) {
                return -1;
            }

            @Override
            void ensureTopics(SyncTask task, DataSource target, String ownerType) {
                topicChecks.incrementAndGet();
            }

            @Override
            Consumer<String, String> openConsumer(Properties consumerProperties) {
                String group = consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
                String owner = group.substring("ds-task-".length(), group.length() - "-bridge".length());
                return new IdleConsumer("__ds_raw_" + owner + "_v1", dyingOwners.contains(Long.valueOf(owner)),
                    failingOwners.contains(Long.valueOf(owner)));
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

    private static DataSource source() {
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setDatabaseName("db");
        return source;
    }

    private static DataSource kafka() {
        DataSource kafka = new DataSource();
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka.example");
        kafka.setPort(9092);
        return kafka;
    }

    /**
     * Polls nothing, slowly enough not to spin. When told to, its first poll throws an Error (nothing
     * catches that: the worker dies) or a KafkaException (the worker records a stop and retries).
     */
    private static final class IdleConsumer extends MockConsumer<String, String> {
        private final boolean dies;
        private final boolean fails;

        private IdleConsumer(String rawTopic, boolean dies, boolean fails) {
            super(OffsetResetStrategy.EARLIEST);
            this.dies = dies;
            this.fails = fails;
            TopicPartition partition = new TopicPartition(rawTopic, 0);
            schedulePollTask(() -> {
                rebalance(List.of(partition));
                updateBeginningOffsets(Map.of(partition, 0L));
                updateEndOffsets(Map.of(partition, 0L));
            });
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration timeout) {
            if (dies) throw new AssertionError("worker thread killed");
            if (fails) throw new KafkaException("broker connection lost");
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return super.poll(timeout);
        }
    }
}
