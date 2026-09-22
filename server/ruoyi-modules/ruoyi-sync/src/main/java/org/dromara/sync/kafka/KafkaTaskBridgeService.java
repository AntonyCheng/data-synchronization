package org.dromara.sync.kafka;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.springframework.stereotype.Component;
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
@Component
public class KafkaTaskBridgeService {

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

    /** Bridge for a platform task; publish metrics are persisted onto its ds_sync_task row. */
    public void start(SyncTask task, DataSource target, String sourceDatabase) {
        start(task, target, sourceDatabase, true);
    }

    /** Task-group items publish the same event contract but do not map to ds_sync_task metrics. */
    public void startGroupItem(SyncTask task, DataSource target, String sourceDatabase) {
        start(task, target, sourceDatabase, false);
    }

    private void start(SyncTask task, DataSource target, String sourceDatabase, boolean persistTaskMetrics) {
        if (task == null || task.getTaskId() == null) throw new ServiceException("Kafka 桥接任务不能为空");
        if (!DataSourceType.isKafka(target)) {
            throw new ServiceException("Kafka 桥接目标数据源无效");
        }
        if (StringUtils.isBlank(task.getTargetTable())) throw new ServiceException("Kafka topic 不能为空");
        List<String> keyFields = SyncColumnSelectionValidator.parseColumns(task.getSyncKeyColumns());
        if (keyFields.isEmpty()) throw new ServiceException("Kafka 任务必须配置可靠同步键");
        // Persisted at save time as the fully expanded, real-cased column list; used to
        // undo the Oracle-compat UPPER CASE folding a GoldenDB JDBC snapshot applies.
        List<String> sourceColumns = SyncColumnSelectionValidator.parseColumns(task.getSelectedColumns());
        ensureTopics(task, target);
        KafkaOutputFormat outputFormat = KafkaOutputFormat.parse(task.getKafkaOutputFormat());
        workers.compute(task.getTaskId(), (taskId, existing) -> {
            if (existing != null && existing.isRunning()) return existing;
            if (existing != null) existing.close();
            Worker worker = new Worker(taskId, KafkaAdminClients.bootstrapServers(target), rawTopic(task), task.getTargetTable(),
                task.getSourceTable(), sourceDatabase, keyFields, sourceColumns, persistTaskMetrics, outputFormat);
            worker.future = executor.submit(worker);
            return worker;
        });
    }

    public void stop(Long taskId) {
        Worker worker = workers.remove(taskId);
        if (worker != null) worker.close();
    }

    public boolean isRunning(Long taskId) {
        Worker worker = workers.get(taskId);
        return worker != null && worker.isRunning();
    }

    /**
     * Whole-database groups own their topic namespace: the topic is named after the source
     * table and the platform creates it here, the same way a relational whole-database
     * target auto-creates its tables. Single/multi-table groups keep the explicit wizard
     * step so the operator controls partitioning, so this is only called for DATABASE scope.
     */
    public void ensureTopicExists(DataSource target, String topic) {
        if (StringUtils.isBlank(topic)) throw new ServiceException("Kafka topic 不能为空");
        try (AdminClient admin = KafkaAdminClients.open(target)) {
            if (admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic)) return;
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            if (ex.getCause() instanceof TopicExistsException) return;
            throw new ServiceException("自动创建 Kafka topic 失败：" + SyncText.safeMessage(ex, ex.getClass().getSimpleName()));
        }
    }

    /** The output topic is user-owned and must exist; only the private raw topic is created here. */
    private void ensureTopics(SyncTask task, DataSource target) {
        String bootstrapServers = KafkaAdminClients.bootstrapServers(target);
        String rawTopic = rawTopic(task);
        try (AdminClient admin = AdminClient.create(KafkaAdminClients.adminProperties(bootstrapServers))) {
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
                    // One partition, deliberately. The raw topic is an internal buffer read by
                    // exactly one single-threaded bridge worker, so extra partitions buy no
                    // throughput - they only cost ordering. A GoldenDB UPDATE arrives as a
                    // DELETE + INSERT pair (see KafkaEventNormalizer); on a multi-partition
                    // topic another row's event can interleave between the two halves and the
                    // normalizer then can't merge them back into an op=UPDATE. A single
                    // partition keeps the stream in binlog order so the pair stays adjacent.
                    admin.createTopics(List.of(new NewTopic(rawTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    if (!(ex.getCause() instanceof TopicExistsException)) throw ex;
                }
            } else {
                var rawDescription = admin.describeTopics(List.of(rawTopic)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(rawTopic);
                if (rawDescription != null && rawDescription.partitions().size() > 1) {
                    log.warn("Raw bridge topic {} has {} partitions; GoldenDB UPDATE merge needs a single "
                            + "partition. Delete the topic while the task is stopped and restart to recreate it.",
                        rawTopic, rawDescription.partitions().size());
                }
            }
        } catch (ServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ServiceException("Kafka topic 预检查失败：" + SyncText.safeMessage(ex, ex.getClass().getSimpleName()));
        }
    }

    /** Private, versioned raw Debezium topic the engine writes and this bridge consumes. */
    public static String rawTopic(SyncTask task) {
        long taskId = task == null || task.getTaskId() == null ? 0L : task.getTaskId();
        int version = task == null || task.getConfigVersion() == null ? 1 : task.getConfigVersion();
        return "__ds_raw_" + taskId + "_v" + version;
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
        private final KafkaOutputFormat outputFormat;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile KafkaConsumer<String, String> consumer;
        private volatile Future<?> future;
        private JsonNode pendingDelete;

        private Worker(Long taskId, String bootstrapServers, String rawTopic, String targetTopic, String sourceTable,
                       String sourceDatabase,
                       List<String> keyFields,
                       List<String> sourceColumns,
                       boolean persistTaskMetrics,
                       KafkaOutputFormat outputFormat) {
            this.taskId = taskId;
            this.bootstrapServers = bootstrapServers;
            this.rawTopic = rawTopic;
            this.targetTopic = targetTopic;
            this.sourceDatabase = sourceDatabase;
            this.sourceTable = TableNames.unqualified(sourceTable);
            this.keyFields = List.copyOf(keyFields);
            this.sourceColumns = List.copyOf(sourceColumns);
            this.persistTaskMetrics = persistTaskMetrics;
            this.outputFormat = outputFormat;
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
                if (persistTaskMetrics) producer.publishForTask(taskId, bootstrapServers, targetTopic, events, outputFormat);
                else producer.publish(bootstrapServers, targetTopic, events, outputFormat);
                return;
            }
            int snapshotCount = 0;
            while (snapshotCount < rawEvents.size()
                && "r".equals(rawEvents.get(snapshotCount).path("op").asText())) snapshotCount++;
            List<KafkaEventNormalizer.NormalizedEvent> events = normalizer.normalize(rawEvents, snapshotCount, keyFields);
            if (persistTaskMetrics) producer.publishForTask(taskId, bootstrapServers, targetTopic, events, outputFormat);
            else producer.publish(bootstrapServers, targetTopic, events, outputFormat);
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
