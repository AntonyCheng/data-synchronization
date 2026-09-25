package org.dromara.sync.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaRawTopic;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.KafkaRawTopicMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The raw-topic side of a bridge start against a stubbed AdminClient: the topic is created when
 * missing, registered for the janitor either way, and never auto-created by the worker's consumer.
 */
@Tag("dev")
class KafkaTaskBridgeRawTopicTest {

    private static final long KAFKA_ID = 10L;

    private final KafkaRawTopicMapper rawTopicMapper = mock(KafkaRawTopicMapper.class);
    private final AdminClient admin = mock(AdminClient.class);
    private final Set<String> topics = new HashSet<>();
    private final List<Properties> consumers = new CopyOnWriteArrayList<>();
    private KafkaTaskBridgeService bridge;

    @AfterEach
    void closeBridge() {
        if (bridge != null) bridge.close();
    }

    @Test
    void aMissingRawTopicIsCreatedAndRegisteredForItsTask() {
        cluster("customer-events");
        bridge = bridge();

        bridge.start(task(1L, 3), kafka(), source());

        verify(admin).createTopics(anyCollection());
        KafkaRawTopic row = registered();
        assertEquals(KAFKA_ID, row.getDataSourceId());
        assertEquals("__ds_raw_1_v3", row.getTopicName());
        assertEquals(KafkaRawTopic.OWNER_TASK, row.getOwnerType());
        assertEquals(1L, row.getOwnerId());
        assertEquals(3, row.getConfigVersion());
        assertNotNull(row.getCreateTime());
    }

    @Test
    void anExistingRawTopicIsAdoptedForAGroupItem() {
        cluster("customer-events", "__ds_raw_70_v4");
        bridge = bridge();

        bridge.startGroupItem(task(70L, 4), kafka(), source());

        verify(admin, never()).createTopics(anyCollection());
        KafkaRawTopic row = registered();
        assertEquals("__ds_raw_70_v4", row.getTopicName());
        assertEquals(KafkaRawTopic.OWNER_GROUP_ITEM, row.getOwnerType());
        assertEquals(70L, row.getOwnerId());
    }

    @Test
    void theBackgroundEntryPointsRegisterToo() {
        cluster("customer-events", "__ds_raw_1_v1", "__ds_raw_70_v1");
        bridge = bridge();

        bridge.tryStart(task(1L, 1), kafka(), source());
        bridge.tryStartGroupItem(task(70L, 1), kafka(), source());

        ArgumentCaptor<KafkaRawTopic> rows = ArgumentCaptor.forClass(KafkaRawTopic.class);
        verify(rawTopicMapper, times(2)).registerIfAbsent(rows.capture());
        assertEquals(List.of(KafkaRawTopic.OWNER_TASK, KafkaRawTopic.OWNER_GROUP_ITEM),
            rows.getAllValues().stream().map(KafkaRawTopic::getOwnerType).toList());
    }

    @Test
    void aFailedRegistrationDoesNotFailTheStart() {
        cluster("customer-events");
        when(rawTopicMapper.registerIfAbsent(any())).thenThrow(new IllegalStateException("metadata database unavailable"));
        bridge = bridge();

        bridge.start(task(1L, 1), kafka(), source());

        assertTrue(bridge.isRunning(1L));
    }

    @Test
    void theWorkerNeverAutoCreatesItsRawTopic() throws InterruptedException {
        cluster("customer-events");
        bridge = bridge();

        bridge.start(task(1L, 1), kafka(), source());

        for (int i = 0; i < 500 && consumers.isEmpty(); i++) Thread.sleep(10);
        Properties consumer = consumers.getFirst();
        assertEquals("false", consumer.getProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG));
        assertEquals(KafkaTaskBridgeService.consumerGroup(1L), consumer.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("ds-task-1-bridge", KafkaTaskBridgeService.consumerGroup(1L));
    }

    private KafkaRawTopic registered() {
        ArgumentCaptor<KafkaRawTopic> row = ArgumentCaptor.forClass(KafkaRawTopic.class);
        verify(rawTopicMapper).registerIfAbsent(row.capture());
        return row.getValue();
    }

    private KafkaTaskBridgeService bridge() {
        return new KafkaTaskBridgeService(mock(KafkaEventNormalizer.class), mock(KafkaEventProducer.class),
            JsonMapper.builder().build(), new KafkaBridgeProperties(), rawTopicMapper) {

            @Override
            AdminClient openAdmin(String bootstrapServers) {
                return admin;
            }

            @Override
            Consumer<String, String> openConsumer(Properties properties) {
                consumers.add(properties);
                return new IdleConsumer();
            }
        };
    }

    /** The broker's topics; every one has a single partition. */
    private void cluster(String... names) {
        topics.addAll(List.of(names));
        ListTopicsResult listing = mock(ListTopicsResult.class);
        when(listing.names()).thenAnswer(inv -> KafkaFuture.completedFuture(Set.copyOf(topics)));
        when(admin.listTopics()).thenReturn(listing);
        when(admin.describeTopics(anyCollection())).thenAnswer(inv -> {
            Collection<String> described = inv.getArgument(0);
            Map<String, TopicDescription> descriptions = new HashMap<>();
            Node broker = new Node(1, "kafka.example", 9092);
            for (String name : described) {
                descriptions.put(name, new TopicDescription(name, false,
                    List.of(new TopicPartitionInfo(0, broker, List.of(broker), List.of(broker)))));
            }
            DescribeTopicsResult result = mock(DescribeTopicsResult.class);
            when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(descriptions));
            return result;
        });
        CreateTopicsResult created = mock(CreateTopicsResult.class);
        when(created.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(created);
    }

    private static SyncTask task(long id, int version) {
        SyncTask task = new SyncTask();
        task.setTaskId(id);
        task.setSourceTable("customers");
        task.setTargetTable("customer-events");
        task.setSelectedColumns("id,name");
        task.setSyncKeyColumns("id");
        task.setConfigVersion(version);
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
        kafka.setSourceId(KAFKA_ID);
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka.example");
        kafka.setPort(9092);
        return kafka;
    }

    /** Polls nothing, slowly enough not to spin. */
    private static final class IdleConsumer extends MockConsumer<String, String> {

        private IdleConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration timeout) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return super.poll(timeout);
        }
    }
}
