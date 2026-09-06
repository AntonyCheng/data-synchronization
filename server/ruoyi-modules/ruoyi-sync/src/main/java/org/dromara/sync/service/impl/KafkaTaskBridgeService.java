package org.dromara.sync.service.impl;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the runtime bridge from SeaTunnel's Debezium raw topic to the PRD event topic.
 * Offsets are committed only after the normalized event has received a broker ack.
 */
@Slf4j
@RequiredArgsConstructor
@Service
class KafkaTaskBridgeService {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private final KafkaEventNormalizer normalizer;
    private final KafkaEventProducer producer;
    private final JsonMapper jsonMapper;
    private final ConcurrentMap<Long, Worker> workers = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "sync-kafka-bridge");
        thread.setDaemon(true);
        return thread;
    });

    void start(SyncTask task, DataSource target) {
        start(task, target, null, true);
    }

    void start(SyncTask task, DataSource target, String sourceDatabase) {
        start(task, target, sourceDatabase, true);
    }

    /** Task-group items publish the same event contract but do not map to ds_sync_task metrics. */
    void startGroupItem(SyncTask task, DataSource target) {
        start(task, target, null, false);
    }

    void startGroupItem(SyncTask task, DataSource target, String sourceDatabase) {
        start(task, target, sourceDatabase, false);
    }

    private void start(SyncTask task, DataSource target, String sourceDatabase, boolean persistTaskMetrics) {
        if (task == null || task.getTaskId() == null) throw new ServiceException("Kafka 桥接任务不能为空");
        if (target == null || !"KAFKA".equalsIgnoreCase(target.getSourceType())) {
            throw new ServiceException("Kafka 桥接目标数据源无效");
        }
        if (StringUtils.isBlank(task.getTargetTable())) throw new ServiceException("Kafka topic 不能为空");
        List<String> keyFields = SyncColumnSelectionValidator.parseColumns(task.getSyncKeyColumns());
        if (keyFields.isEmpty()) throw new ServiceException("Kafka 任务必须配置可靠同步键");
        // Persisted at save time as the fully expanded, real-cased column list; used to
        // undo the Oracle-compat UPPER CASE folding a GoldenDB JDBC snapshot applies.
        List<String> sourceColumns = SyncColumnSelectionValidator.parseColumns(task.getSelectedColumns());
        ensureTopics(task, target);
        workers.compute(task.getTaskId(), (taskId, existing) -> {
            if (existing != null && existing.isRunning()) return existing;
            if (existing != null) existing.close();
            Worker worker = new Worker(taskId, bootstrapServers(target), rawTopic(task), task.getTargetTable(),
                task.getSourceTable(), sourceDatabase, keyFields, sourceColumns, persistTaskMetrics);
            worker.future = executor.submit(worker);
            return worker;
        });
    }

    void stop(Long taskId) {
        Worker worker = workers.remove(taskId);
        if (worker != null) worker.close();
    }

    boolean isRunning(Long taskId) {
        Worker worker = workers.get(taskId);
        return worker != null && worker.isRunning();
    }

    /**
     * Whole-database groups own their topic namespace: the topic is named after the source
     * table and the platform creates it here, the same way a relational whole-database
     * target auto-creates its tables. Single/multi-table groups keep the explicit wizard
     * step so the operator controls partitioning, so this is only called for DATABASE scope.
     */
    void ensureTopicExists(DataSource target, String topic) {
        if (StringUtils.isBlank(topic)) throw new ServiceException("Kafka topic 不能为空");
        try (AdminClient admin = AdminClient.create(adminProperties(bootstrapServers(target)))) {
            if (admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic)) return;
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            if (ex.getCause() instanceof TopicExistsException) return;
            throw new ServiceException("自动创建 Kafka topic 失败：" + safeMessage(ex));
        }
    }

    /** The output topic is user-owned and must exist; only the private raw topic is created here. */
    void ensureTopics(SyncTask task, DataSource target) {
        String bootstrapServers = bootstrapServers(target);
        String rawTopic = rawTopic(task);
        try (AdminClient admin = AdminClient.create(adminProperties(bootstrapServers))) {
            Set<String> knownTopics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if (!knownTopics.contains(task.getTargetTable())) {
                throw new ServiceException("Kafka 目标 topic 不存在或当前凭证无查看权限：" + task.getTargetTable());
            }
            var targetDescription = admin.describeTopics(List.of(task.getTargetTable())).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(task.getTargetTable());
            if (targetDescription == null || targetDescription.partitions().isEmpty()) {
                throw new ServiceException("Kafka 目标 topic 没有可用分区：" + task.getTargetTable());
            }
            if (!knownTopics.contains(rawTopic)) {
                try {
                    admin.createTopics(List.of(new NewTopic(rawTopic, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    if (!(ex.getCause() instanceof TopicExistsException)) throw ex;
                }
            }
        } catch (ServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ServiceException("Kafka topic 预检查失败：" + safeMessage(ex));
        }
    }

    static String rawTopic(SyncTask task) {
        long taskId = task == null || task.getTaskId() == null ? 0L : task.getTaskId();
        int version = task == null || task.getConfigVersion() == null ? 1 : task.getConfigVersion();
        return "__ds_raw_" + taskId + "_v" + version;
    }

    static String bootstrapServers(DataSource target) {
        if (target == null || StringUtils.isBlank(target.getHost()) || target.getPort() == null) {
            throw new ServiceException("Kafka broker 地址不能为空");
        }
        return target.getHost() + ':' + target.getPort();
    }

    private static Properties adminProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        return properties;
    }

    private static String safeMessage(Exception ex) {
        String message = ex.getMessage();
        return StringUtils.isBlank(message) ? ex.getClass().getSimpleName() : message;
    }

    @PreDestroy
    void close() {
        workers.values().forEach(Worker::close);
        workers.clear();
        executor.shutdownNow();
    }

    private final class Worker implements Runnable {
        private final Long taskId;
        private final String bootstrapServers;
        private final String rawTopic;
        private final String targetTopic;
        private final String sourceDatabase;
        private final String sourceTable;
        private final List<String> keyFields;
        private final List<String> sourceColumns;
        private final boolean persistTaskMetrics;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile KafkaConsumer<String, String> consumer;
        private volatile Future<?> future;
        private JsonNode pendingDelete;

        private Worker(Long taskId, String bootstrapServers, String rawTopic, String targetTopic, String sourceTable,
                       String sourceDatabase,
                       List<String> keyFields,
                       List<String> sourceColumns,
                       boolean persistTaskMetrics) {
            this.taskId = taskId;
            this.bootstrapServers = bootstrapServers;
            this.rawTopic = rawTopic;
            this.targetTopic = targetTopic;
            this.sourceDatabase = sourceDatabase;
            int separator = sourceTable == null ? -1 : sourceTable.lastIndexOf('.');
            this.sourceTable = separator < 0 ? sourceTable : sourceTable.substring(separator + 1);
            this.keyFields = List.copyOf(keyFields);
            this.sourceColumns = List.copyOf(sourceColumns);
            this.persistTaskMetrics = persistTaskMetrics;
        }

        @Override
        public void run() {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ds-task-" + taskId + "-bridge");
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            try (KafkaConsumer<String, String> opened = new KafkaConsumer<>(properties)) {
                consumer = opened;
                opened.subscribe(List.of(rawTopic));
                while (running.get()) {
                    ConsumerRecords<String, String> records = opened.poll(POLL_TIMEOUT);
                    if (!records.isEmpty()) {
                        List<JsonNode> rawEvents = new java.util.ArrayList<>(records.count());
                        for (ConsumerRecord<String, String> record : records) {
                            if (!running.get()) return;
                            rawEvents.add(jsonMapper.readTree(record.value()));
                        }
                        // MySQL CDC can emit UPDATE as DELETE and CREATE in separate
                        // poll batches. Keep a trailing DELETE uncommitted until the
                        // next batch gives the normalizer a chance to merge it.
                        if (pendingDelete != null) {
                            rawEvents.addFirst(pendingDelete);
                            pendingDelete = null;
                        }
                        if (!rawEvents.isEmpty() && "d".equals(rawEvents.getLast().path("op").asText())) {
                            pendingDelete = rawEvents.removeLast();
                        }
                        publish(rawEvents);
                        if (pendingDelete == null) opened.commitSync();
                    } else if (pendingDelete != null) {
                        // A standalone DELETE is released after one empty poll. The
                        // deferred offset remains uncommitted until its broker ack.
                        publish(List.of(pendingDelete));
                        pendingDelete = null;
                        opened.commitSync();
                    }
                }
            } catch (Exception ex) {
                if (running.get()) log.error("Kafka task bridge stopped unexpectedly: taskId={}", taskId, ex);
            } finally {
                consumer = null;
                running.set(false);
            }
        }

        private boolean isRunning() {
            return running.get() && future != null && !future.isDone();
        }

        private void publish(List<JsonNode> rawEvents) {
            if (rawEvents.isEmpty()) return;
            boolean snapshotRows = rawEvents.stream().allMatch(event -> event != null && !event.has("op"));
            if (snapshotRows) {
                String database = StringUtils.isBlank(sourceDatabase) ? "unknown" : sourceDatabase;
                List<KafkaEventNormalizer.NormalizedEvent> events = normalizer.normalizeSnapshotRows(
                    rawEvents, database, sourceTable, keyFields, sourceColumns);
                if (persistTaskMetrics) producer.publishForTask(taskId, bootstrapServers, targetTopic, events);
                else producer.publish(bootstrapServers, targetTopic, events);
                return;
            }
            int snapshotCount = 0;
            while (snapshotCount < rawEvents.size()
                && "r".equals(rawEvents.get(snapshotCount).path("op").asText())) snapshotCount++;
            List<KafkaEventNormalizer.NormalizedEvent> events = normalizer.normalize(rawEvents, snapshotCount, keyFields);
            if (persistTaskMetrics) producer.publishForTask(taskId, bootstrapServers, targetTopic, events);
            else producer.publish(bootstrapServers, targetTopic, events);
        }

        private void close() {
            running.set(false);
            KafkaConsumer<String, String> opened = consumer;
            if (opened != null) opened.wakeup();
            Future<?> current = future;
            if (current != null) {
                current.cancel(true);
                try {
                    current.get(3, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // The consumer may already have stopped after wakeup/cancellation.
                }
            }
        }
    }
}
