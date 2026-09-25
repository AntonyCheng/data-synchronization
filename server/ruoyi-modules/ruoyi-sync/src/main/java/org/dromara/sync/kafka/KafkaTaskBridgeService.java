package org.dromara.sync.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.support.SourceTimeZones;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns the runtime bridge from SeaTunnel's Debezium raw topic to the PRD event topic.
 * Offsets are committed only after the normalized event has received a broker ack.
 *
 * <p>Capacity: at most {@code sync.kafka-bridge.max-workers} workers (one thread + one consumer
 * each) per instance. An owner that already has a worker keeps its slot when the worker is
 * replaced. When the pool is full:
 * <ul>
 *   <li>{@link #start} / {@link #startGroupItem} - an operator is about to submit an engine job -
 *       throw, so the start is refused before anything is submitted;</li>
 *   <li>{@link #tryStart} / {@link #tryStartGroupItem} - the owner's engine job already runs
 *       (reconciler, status-refresh heal, startup recovery) - park the owner and return
 *       {@code false}. Failing it would not stop the job, and the raw topic keeps its events;
 *       the bridge catches up from its committed offset once a slot frees. Parked owners are
 *       served before new operator starts, and the check is made before any AdminClient or
 *       consumer is created, so retrying every pass costs nothing.</li>
 * </ul>
 */
@Slf4j
@Component
public class KafkaTaskBridgeService {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    /** Bounded by POLL_TIMEOUT plus one commit round-trip; anything longer means the worker is wedged. */
    private static final Duration GRACEFUL_STOP_TIMEOUT = Duration.ofSeconds(5);
    private static final String MAX_WORKERS_KEY = "sync.kafka-bridge.max-workers";
    private final KafkaEventNormalizer normalizer;
    private final KafkaEventProducer producer;
    private final JsonMapper jsonMapper;
    private final int maxWorkers;
    private final ConcurrentMap<Long, Worker> workers = new ConcurrentHashMap<>();
    /** RUNNING owners refused a worker because the pool was full; they get the next free slots. */
    private final Set<Long> parked = ConcurrentHashMap.newKeySet();
    /** Guards "check capacity, then claim a slot" and every change to {@link #parked}. */
    private final Object admission = new Object();
    /**
     * Admission keeps at most maxWorkers workers; the pool size is the hard thread bound. A worker
     * queues only for the few seconds a stopped predecessor needs to release its thread. Idle
     * threads time out, so an instance without Kafka tasks holds none.
     */
    private final ThreadPoolExecutor executor;

    public KafkaTaskBridgeService(KafkaEventNormalizer normalizer, KafkaEventProducer producer, JsonMapper jsonMapper,
                                  KafkaBridgeProperties properties) {
        this.normalizer = normalizer;
        this.producer = producer;
        this.jsonMapper = jsonMapper;
        this.maxWorkers = Math.max(1, properties.getMaxWorkers());
        AtomicInteger threads = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(maxWorkers, maxWorkers, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
            Thread thread = new Thread(runnable, "sync-kafka-bridge-" + threads.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        this.executor.allowCoreThreadTimeOut(true);
    }

    /**
     * Operator start of a platform task; publish metrics are persisted onto its ds_sync_task row. Throws when full.
     * {@code source} is the task's source data source: its database names bounded snapshot rows, its time
     * zone renders the {@code TIMESTAMP} values of CDC records (see {@link KafkaRawRecordReader}).
     */
    public void start(SyncTask task, DataSource target, DataSource source) {
        start(task, target, source, true, true);
    }

    /** Operator start of a task-group item (no ds_sync_task metrics). Throws when full. */
    public void startGroupItem(SyncTask task, DataSource target, DataSource source) {
        start(task, target, source, false, true);
    }

    /** Bridge for a task whose engine job already runs; {@code false} = parked until a slot frees. */
    public boolean tryStart(SyncTask task, DataSource target, DataSource source) {
        return start(task, target, source, true, false);
    }

    /** Bridge for a group item whose engine job already runs; {@code false} = parked until a slot frees. */
    public boolean tryStartGroupItem(SyncTask task, DataSource target, DataSource source) {
        return start(task, target, source, false, false);
    }

    private boolean start(SyncTask task, DataSource target, DataSource source, boolean persistTaskMetrics,
                          boolean refuseWhenFull) {
        if (task == null || task.getTaskId() == null) throw new ServiceException("Kafka 桥接任务不能为空");
        if (!DataSourceType.isKafka(target)) {
            throw new ServiceException("Kafka 桥接目标数据源无效");
        }
        if (StringUtils.isBlank(task.getTargetTable())) throw new ServiceException("Kafka topic 不能为空");
        List<String> keyFields = SyncColumnSelectionValidator.parseColumns(task.getSyncKeyColumns());
        if (keyFields.isEmpty()) throw new ServiceException("Kafka 任务必须配置可靠同步键");
        // Persisted at save time as the fully expanded, real-cased column list; used to
        // undo the Oracle-compat UPPER CASE folding a GoldenDB JDBC snapshot applies, and
        // to cut CDC records (always the whole row) down to the selection.
        List<String> sourceColumns = SyncColumnSelectionValidator.parseColumns(task.getSelectedColumns());
        String sourceDatabase = source == null ? null : source.getDatabaseName();
        ZoneId sourceZone = sourceZone(source);
        Long ownerId = task.getTaskId();
        // Before the topic precheck: a refused owner costs no AdminClient round trip.
        if (!admit(ownerId, refuseWhenFull)) return false;
        ensureTopics(task, target);
        KafkaOutputFormat outputFormat = KafkaOutputFormat.parse(task.getKafkaOutputFormat());
        boolean wasParked;
        synchronized (admission) {
            // Again, atomically with the claim: another start may have taken the last slot meanwhile.
            if (!admit(ownerId, refuseWhenFull)) return false;
            workers.compute(ownerId, (taskId, existing) -> {
                if (existing != null && existing.isRunning()) return existing;
                if (existing != null) existing.close();
                Worker worker = new Worker(taskId, KafkaAdminClients.bootstrapServers(target), rawTopic(task), task.getTargetTable(),
                    task.getSourceTable(), sourceDatabase, sourceZone, keyFields, sourceColumns, persistTaskMetrics, outputFormat);
                worker.future = executor.submit(worker);
                return worker;
            });
            wasParked = parked.remove(ownerId);
        }
        if (wasParked) log.info("kafka bridge got a free slot: owner {} is bridged again", ownerId);
        return true;
    }

    /**
     * The zone the engine job was given as {@code server-time-zone}; the generator refuses to build a
     * job for an invalid one, so an invalid value here was written around the service.
     */
    private static ZoneId sourceZone(DataSource source) {
        String zone = SourceTimeZones.effective(source);
        if (!SourceTimeZones.isValid(zone)) throw new ServiceException("源数据源的服务器时区“" + zone + "”无效，Kafka 桥接无法换算 TIMESTAMP");
        return ZoneId.of(zone);
    }

    /**
     * True when the owner may hold a worker. When it may not, an operator start is refused with
     * an exception and a background start parks the owner, logging only the transition.
     */
    private boolean admit(Long ownerId, boolean refuseWhenFull) {
        synchronized (admission) {
            if (workers.containsKey(ownerId)) return true;
            // Operator starts leave the slots of parked owners alone: those already run on the engine.
            int reserved = refuseWhenFull ? parked.size() - (parked.contains(ownerId) ? 1 : 0) : 0;
            if (workers.size() + reserved < maxWorkers) return true;
            if (refuseWhenFull) throw new ServiceException(capacityMessage(1));
            if (parked.add(ownerId)) {
                log.warn("kafka bridge pool full ({}/{}): owner {} keeps running on the engine without a bridge, "
                    + "its raw topic buffers the events until a slot frees ({})", workers.size(), maxWorkers, ownerId, MAX_WORKERS_KEY);
            }
            return false;
        }
    }

    /**
     * Operator pre-check before starting several owners in one go (a task group resume), so the
     * whole operation is refused before its first job is submitted instead of failing halfway.
     */
    public void requireCapacity(Collection<Long> ownerIds) {
        synchronized (admission) {
            long needed = ownerIds.stream().distinct().filter(id -> !workers.containsKey(id)).count();
            long reserved = parked.stream().filter(id -> !ownerIds.contains(id)).count();
            if (needed > 0 && workers.size() + reserved + needed > maxWorkers) {
                throw new ServiceException(capacityMessage(needed));
            }
        }
    }

    private String capacityMessage(long needed) {
        int used = workers.size();
        int waiting = parked.size();
        String usage = needed > 1
            ? "Kafka 桥接容量不足（已用 " + used + "/" + maxWorkers + "，本次需要 " + needed + " 个"
            : "Kafka 桥接容量已满（" + used + "/" + maxWorkers;
        return usage + (waiting > 0 ? "，另有 " + waiting + " 个运行中的任务/表项在排队等待桥接" : "")
            + "）：每个运行中的 Kafka 任务/表项在每个后端实例上各占用 1 个桥接 worker。请调大 "
            + MAX_WORKERS_KEY + "，或先停止其他 Kafka 任务后重试";
    }

    /**
     * What an operator should see as the last error of an owner: the stored error, prefixed with
     * a notice while this instance holds the owner parked without a bridge. Read-time only - the
     * status refresh rewrites last_error every cycle, and the parked state is per instance.
     */
    public String decorateLastError(Long ownerId, String lastError) {
        if (ownerId == null || !parked.contains(ownerId)) return lastError;
        String notice = "Kafka 桥接等待容量：本实例桥接已满（" + workers.size() + "/" + maxWorkers
            + "），引擎作业仍在运行、变更暂存在 raw topic 中，腾出空位后自动续传；如需立即恢复请调大 "
            + MAX_WORKERS_KEY + " 或停止其他 Kafka 任务";
        return StringUtils.isBlank(lastError) ? notice : notice + "；" + lastError;
    }

    public void stop(Long taskId) {
        Worker worker;
        synchronized (admission) {
            worker = workers.remove(taskId);
            parked.remove(taskId);
        }
        if (worker != null) worker.close();
    }

    public boolean isRunning(Long taskId) {
        Worker worker = workers.get(taskId);
        return worker != null && worker.isRunning();
    }

    /**
     * Ids (task ids or group item ids) this process has a worker for, live or not, or holds
     * parked waiting for one - everything {@link #stop} should retire once the owner stops.
     */
    public Set<Long> localOwnerIds() {
        Set<Long> owners = new HashSet<>(workers.keySet());
        owners.addAll(parked);
        return Set.copyOf(owners);
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

    /** The output topic is user-owned and must exist; only the private raw topic is created here. Package-private for tests. */
    void ensureTopics(SyncTask task, DataSource target) {
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
                    // throughput - they only cost ordering. A single partition keeps the stream in
                    // binlog order: the initial load before every change, each key's changes in
                    // order, and the DELETE + CREATE halves of an UPDATE adjacent where the engine
                    // splits one (always with SeaTunnel's DEBEZIUM_JSON format, which jobs started
                    // before compatible_debezium_json still write; with Debezium's records only when
                    // the primary key changed and the sync key is another unique key).
                    admin.createTopics(List.of(new NewTopic(rawTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    if (!(ex.getCause() instanceof TopicExistsException)) throw ex;
                }
            } else {
                var rawDescription = admin.describeTopics(List.of(rawTopic)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(rawTopic);
                if (rawDescription != null && rawDescription.partitions().size() > 1) {
                    log.warn("Raw bridge topic {} has {} partitions; the UPDATE merge needs a single "
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

    /** The consumer a worker reads its raw topic with; package-private for tests. */
    Consumer<String, String> openConsumer(Properties properties) {
        return new KafkaConsumer<>(properties);
    }

    @PreDestroy
    void close() {
        workers.values().forEach(Worker::close);
        workers.clear();
        parked.clear();
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
        private final KafkaRawRecordReader reader;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile Consumer<String, String> consumer;
        private volatile Future<?> future;
        private JsonNode pendingDelete;

        private Worker(Long taskId, String bootstrapServers, String rawTopic, String targetTopic, String sourceTable,
                       String sourceDatabase,
                       ZoneId sourceZone,
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
            this.reader = new KafkaRawRecordReader(jsonMapper, sourceZone);
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
            try (Consumer<String, String> opened = openConsumer(properties)) {
                consumer = opened;
                opened.subscribe(List.of(rawTopic));
                while (running.get()) {
                    ConsumerRecords<String, String> records = opened.poll(POLL_TIMEOUT);
                    if (!records.isEmpty()) {
                        List<JsonNode> rawEvents = new java.util.ArrayList<>(records.count());
                        for (ConsumerRecord<String, String> record : records) {
                            if (!running.get()) return;
                            JsonNode event = reader.read(record.value());
                            if (event != null) rawEvents.add(event);
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
            // MySQL-CDC (FULL_CDC / INCREMENTAL). The engine writes Debezium's own records
            // (compatible_debezium_json), so an initial-load row is op=r and published as SNAPSHOT.
            // One engine run reads the binlog only after its whole initial load; a restarted or
            // reinitialized job starts a new initial load in the same raw topic. A job started
            // before that format (SeaTunnel's DEBEZIUM_JSON) wrote its initial load as op=c,
            // indistinguishable from a binlog insert, and that stays CDC. See docs/kafka-event-formats.md.
            int snapshotCount = 0;
            while (snapshotCount < rawEvents.size()
                && "r".equals(rawEvents.get(snapshotCount).path("op").asText())) snapshotCount++;
            List<KafkaEventNormalizer.NormalizedEvent> events = normalizer.normalize(rawEvents, snapshotCount, keyFields, sourceColumns);
            if (persistTaskMetrics) producer.publishForTask(taskId, bootstrapServers, targetTopic, events, outputFormat);
            else producer.publish(bootstrapServers, targetTopic, events, outputFormat);
        }

        /**
         * Stop cooperatively first: {@code wakeup()} makes the blocked {@code poll()} /
         * {@code commitSync()} throw, the loop exits and try-with-resources closes the
         * consumer cleanly (leaving the group without a rebalance timeout). Interrupting
         * straight away, as this used to, made the Kafka client log an ERROR with a stack
         * trace on every ordinary pause / stop. The interrupt is kept only as a last resort
         * for a worker stuck in a producer ack wait, which {@code wakeup()} cannot unblock.
         */
        private void close() {
            running.set(false);
            Consumer<String, String> opened = consumer;
            if (opened != null) opened.wakeup();
            Future<?> current = future;
            if (current == null) return;
            try {
                current.get(GRACEFUL_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                current.cancel(true);
            } catch (Exception ignored) {
                // Already finished (possibly with the WakeupException we asked for).
            }
        }
    }
}
