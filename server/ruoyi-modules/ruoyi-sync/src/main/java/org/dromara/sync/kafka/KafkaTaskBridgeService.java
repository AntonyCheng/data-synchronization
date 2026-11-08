package org.dromara.sync.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.KafkaRawTopic;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.support.SourceTimeZones;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Owns the runtime bridge from SeaTunnel's Debezium raw topic to the PRD event topic.
 * Offsets are committed only after the normalized event has received a broker ack.
 *
 * <p>Delivery: the bridge never moves past an event it could not publish. When publishing stops - the
 * output topic refuses an event (too large, no ACL), the broker fails, a raw record is unreadable - the
 * worker commits the raw offset up to that event, records why on the owner row
 * ({@code kafka_bridge_error}, which the status refresh shows and {@code SyncAlertNotifier} announces)
 * and tries the same event again after a growing wait ({@code sync.kafka-bridge.retry-*}); the record
 * is cleared once it goes through. The worker stays alive meanwhile, so the reconciler and the status
 * refresh leave it alone - they used to restart a dead worker every 30 s, and it republished the
 * events behind the failed one each time.
 *
 * <p>Position: the consumer runs with {@code auto.offset.reset=none}. When Kafka has no valid offset
 * for the raw topic, the worker decides itself ({@code Worker#position}) and never jumps silently: a
 * position retention already deleted, or one past the end of a recreated topic, is reported with
 * {@link #RAW_TOPIC_GAP_PREFIX}, and the status refresh parks the owner for reinitialization.
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
 *
 * <p>Every start registers the owner's raw topic in {@code ds_kafka_raw_topic}, the list
 * {@code KafkaRawTopicJanitor} deletes superseded raw topics from.
 */
@Slf4j
@Component
public class KafkaTaskBridgeService {

    public static final String RAW_TOPIC_PREFIX = "__ds_raw_";
    /**
     * Starts the stop reason of a bridge whose raw-topic position is gone - events the bridge never
     * published can no longer be read. Only a new initial load repairs that, so the status refresh
     * parks such an owner (REINITIALIZE_REQUIRED, or FAILED for a table item) instead of retrying.
     */
    public static final String RAW_TOPIC_GAP_PREFIX = "Kafka raw topic 位点越界：";
    /** Starts every other stop reason: {@code Kafka 桥接已暂停发布：停在 raw offset 7，<why>}. */
    public static final String STOPPED_PREFIX = "Kafka 桥接已暂停发布：";
    private static final String REINITIALIZE_HINT = "。需要重新初始化：单表任务请停止后重新启动或重新初始化，任务组表项请「重新初始化该表」";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    /** Bounded by POLL_TIMEOUT plus one commit round-trip; anything longer means the worker is wedged. */
    private static final Duration GRACEFUL_STOP_TIMEOUT = Duration.ofSeconds(5);
    /** How far the registry's copy of the committed raw offset may trail the consumer group's. */
    private static final long OFFSET_SAVE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    /** Head room of the raw topic's {@code max.message.bytes} over the engine producer's record limit (batch framing). */
    private static final int RAW_TOPIC_FRAMING_BYTES = 1024;
    private static final int KAFKA_DEFAULT_MAX_BYTES = 1024 * 1024;
    private static final String MAX_WORKERS_KEY = "sync.kafka-bridge.max-workers";
    private final KafkaEventNormalizer normalizer;
    private final KafkaEventProducer producer;
    private final JsonMapper jsonMapper;
    private final KafkaBridgeProperties properties;
    private final SeaTunnelProperties engineProperties;
    private final KafkaBridgeStateStore stateStore;
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
                                  KafkaBridgeProperties properties, SeaTunnelProperties engineProperties,
                                  KafkaBridgeStateStore stateStore) {
        this.normalizer = normalizer;
        this.producer = producer;
        this.jsonMapper = jsonMapper;
        this.properties = properties;
        this.engineProperties = engineProperties;
        this.stateStore = stateStore;
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
     * {@code freshRun}: the job about to be submitted starts a new run (start, reinitialize) rather than
     * resuming a savepoint, so its new initial load may begin at the earliest retained raw record when
     * the bridge's old position is unreadable.
     */
    public void start(SyncTask task, DataSource target, DataSource source, boolean freshRun) {
        start(task, target, source, true, true, freshRun);
    }

    /** Operator start of a task-group item (no ds_sync_task metrics). Throws when full. See {@link #start}. */
    public void startGroupItem(SyncTask task, DataSource target, DataSource source, boolean freshRun) {
        start(task, target, source, false, true, freshRun);
    }

    /** Bridge for a task whose engine job already runs; {@code false} = parked until a slot frees. */
    public boolean tryStart(SyncTask task, DataSource target, DataSource source) {
        return start(task, target, source, true, false, false);
    }

    /** Bridge for a group item whose engine job already runs; {@code false} = parked until a slot frees. */
    public boolean tryStartGroupItem(SyncTask task, DataSource target, DataSource source) {
        return start(task, target, source, false, false, false);
    }

    private boolean start(SyncTask task, DataSource target, DataSource source, boolean persistTaskMetrics,
                          boolean refuseWhenFull, boolean freshRun) {
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
        String ownerType = persistTaskMetrics ? KafkaRawTopic.OWNER_TASK : KafkaRawTopic.OWNER_GROUP_ITEM;
        // Before the topic precheck: a refused owner costs no AdminClient round trip.
        if (!admit(ownerId, refuseWhenFull)) return false;
        ensureTopics(task, target, ownerType);
        if (refuseWhenFull) {
            // An operator starts or resumes the owner: whatever stopped its previous bridge is history,
            // and a worker that finds it again records it again.
            stateStore.clearBlocked(ownerType, ownerId);
            if (freshRun) stateStore.allowReset(target.getSourceId(), rawTopic(task));
        }
        KafkaOutputFormat outputFormat = KafkaOutputFormat.parse(task.getKafkaOutputFormat());
        boolean wasParked;
        synchronized (admission) {
            // Again, atomically with the claim: another start may have taken the last slot meanwhile.
            if (!admit(ownerId, refuseWhenFull)) return false;
            workers.compute(ownerId, (taskId, existing) -> {
                if (existing != null && existing.isRunning()) return existing;
                if (existing != null) existing.close();
                Worker worker = new Worker(taskId, ownerType, target.getSourceId(), KafkaAdminClients.bootstrapServers(target),
                    rawTopic(task), task.getTargetTable(), task.getSourceTable(), sourceDatabase, sourceZone, keyFields,
                    sourceColumns, persistTaskMetrics, outputFormat);
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

    /** True when {@code bridgeError} says the owner's raw-topic position is gone; see {@link #RAW_TOPIC_GAP_PREFIX}. */
    public static boolean isRawTopicGap(String bridgeError) {
        return bridgeError != null && bridgeError.startsWith(RAW_TOPIC_GAP_PREFIX);
    }

    /**
     * The end-to-end lag to show for an owner whose bridge stopped publishing at {@code stoppedSince}
     * (null while it publishes): the lag stored at the last event that went through froze there,
     * while the events behind the stop have been waiting at least since it.
     */
    public static Long effectiveLagSeconds(Long storedLag, LocalDateTime stoppedSince, LocalDateTime now) {
        if (stoppedSince == null) return storedLag;
        long waited = Math.max(0, Duration.between(stoppedSince, now).getSeconds());
        return storedLag == null ? waited : Math.max(storedLag, waited);
    }

    /**
     * The backlog to show for an owner whose bridge stopped publishing ({@code stoppedSince} non-null):
     * the engine counts only what it has not yet written to the raw topic, which a stopped bridge
     * leaves at 0, so the raw records waiting behind the stop - counted at its last attempt - are added.
     */
    public static Long effectiveBacklogRows(Long engineBacklog, LocalDateTime stoppedSince, Long bridgeBacklog) {
        if (stoppedSince == null || bridgeBacklog == null) return engineBacklog;
        return (engineBacklog == null ? 0 : engineBacklog) + bridgeBacklog;
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

    /**
     * The output topic is user-owned and must exist; only the private raw topic is created here,
     * and then registered for cleanup - also when it already existed, so raw topics from before the
     * registry are adopted the next time their owner's bridge starts. Package-private for tests.
     */
    void ensureTopics(SyncTask task, DataSource target, String ownerType) {
        String bootstrapServers = KafkaAdminClients.bootstrapServers(target);
        String rawTopic = rawTopic(task);
        try (AdminClient admin = openAdmin(bootstrapServers)) {
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
                createRawTopic(admin, rawTopic);
            } else {
                var rawDescription = admin.describeTopics(List.of(rawTopic)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(rawTopic);
                if (rawDescription != null && rawDescription.partitions().size() > 1) {
                    log.warn("Raw bridge topic {} has {} partitions; the UPDATE merge needs a single "
                            + "partition. Delete the topic while the task is stopped and restart to recreate it.",
                        rawTopic, rawDescription.partitions().size());
                }
                alignRawTopicLimit(admin, rawTopic);
            }
        } catch (ServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ServiceException("Kafka topic 预检查失败：" + SyncText.safeMessage(ex, ex.getClass().getSimpleName()));
        }
        registerRawTopic(task, target, rawTopic, ownerType);
    }

    /**
     * One partition, deliberately. The raw topic is an internal buffer read by exactly one
     * single-threaded bridge worker, so extra partitions buy no throughput - they only cost ordering.
     * A single partition keeps the stream in binlog order: the initial load before every change, each
     * key's changes in order, and the DELETE + CREATE halves of an UPDATE adjacent where the engine
     * splits one (always with SeaTunnel's DEBEZIUM_JSON format, which jobs started before
     * compatible_debezium_json still write; with Debezium's records only when the primary key changed
     * and the sync key is another unique key; and GoldenDB writes every UPDATE that way).
     *
     * <p>Its {@code max.message.bytes} makes room for the engine's largest record
     * ({@code sync.engine.kafka-max-record-bytes}); Kafka's 1 MB default refuses a row with a large
     * TEXT / BLOB / JSON value. A cluster whose policy forbids the setting gets the topic without it -
     * a record above the broker's limit then fails the engine job, it is never dropped.
     */
    private void createRawTopic(AdminClient admin, String rawTopic) throws Exception {
        try {
            admin.createTopics(List.of(new NewTopic(rawTopic, 1, (short) 1)
                    .configs(Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(rawTopicMaxMessageBytes())))))
                .all().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof TopicExistsException) return;
            if (!(cause instanceof PolicyViolationException || cause instanceof InvalidConfigurationException
                || cause instanceof InvalidRequestException)) throw ex;
            log.warn("raw topic {} created without max.message.bytes={} ({}): a record above the broker default fails the engine job",
                rawTopic, rawTopicMaxMessageBytes(), cause.getMessage());
            try {
                admin.createTopics(List.of(new NewTopic(rawTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            } catch (Exception retry) {
                if (!(retry.getCause() instanceof TopicExistsException)) throw retry;
            }
        }
    }

    /** Raises the {@code max.message.bytes} of a raw topic created before the setting (or by hand); best effort. */
    private void alignRawTopicLimit(AdminClient admin, String rawTopic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, rawTopic);
        int wanted = rawTopicMaxMessageBytes();
        try {
            Config config = admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
            ConfigEntry entry = config == null ? null : config.get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
            if (entry != null && StringUtils.isNotBlank(entry.value()) && Long.parseLong(entry.value().trim()) >= wanted) return;
            admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                    new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(wanted)), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);
            log.info("raw topic {} max.message.bytes raised to {}", rawTopic, wanted);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            log.warn("raw topic {} max.message.bytes not raised to {}: {}", rawTopic, wanted, ex.getMessage());
        }
    }

    private int rawTopicMaxMessageBytes() {
        return Math.max(KAFKA_DEFAULT_MAX_BYTES, engineProperties.getKafkaMaxRecordBytes()) + RAW_TOPIC_FRAMING_BYTES;
    }

    /**
     * Best effort: a registration that fails only postpones the cleanup of this topic, and the
     * owner's next bridge (re)start registers it again - not worth refusing a start over.
     */
    private void registerRawTopic(SyncTask task, DataSource target, String rawTopic, String ownerType) {
        try {
            KafkaRawTopic row = new KafkaRawTopic();
            row.setDataSourceId(target.getSourceId());
            row.setTopicName(rawTopic);
            row.setOwnerType(ownerType);
            row.setOwnerId(task.getTaskId());
            row.setConfigVersion(configVersion(task));
            row.setCreateTime(LocalDateTime.now());
            if (stateStore.register(row)) {
                log.info("raw topic {} registered for cleanup ({} {})", rawTopic, ownerType, task.getTaskId());
            }
        } catch (RuntimeException ex) {
            log.warn("raw topic {} could not be registered for cleanup, retried on the next bridge start: {}",
                rawTopic, ex.getMessage());
        }
    }

    /** The output topic's {@code max.message.bytes}, or -1 when it cannot be read. Package-private for tests. */
    long targetTopicMaxMessageBytes(String bootstrapServers, String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (AdminClient admin = openAdmin(bootstrapServers)) {
            Config config = admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
            ConfigEntry entry = config == null ? null : config.get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
            return entry == null || StringUtils.isBlank(entry.value()) ? -1 : Long.parseLong(entry.value().trim());
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            log.debug("max.message.bytes of topic {} not readable: {}", topic, ex.getMessage());
            return -1;
        }
    }

    /** Private, versioned raw Debezium topic the engine writes and this bridge consumes. */
    public static String rawTopic(SyncTask task) {
        long taskId = task == null || task.getTaskId() == null ? 0L : task.getTaskId();
        return RAW_TOPIC_PREFIX + taskId + "_v" + configVersion(task);
    }

    private static int configVersion(SyncTask task) {
        return task == null || task.getConfigVersion() == null ? 1 : task.getConfigVersion();
    }

    /** The consumer group every instance's worker for one owner (task id or group item id) joins. */
    public static String consumerGroup(Long ownerId) {
        return "ds-task-" + ownerId + "-bridge";
    }

    /** Package-private for tests. */
    AdminClient openAdmin(String bootstrapServers) {
        return AdminClient.create(KafkaAdminClients.adminProperties(bootstrapServers));
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

    /** A raw record that carries a row change, with its offset. */
    private record Entry(long offset, JsonNode event) {
    }

    private final class Worker implements Runnable {
        private final Long taskId;
        private final String ownerType;
        private final Long targetSourceId;
        private final String bootstrapServers;
        private final String rawTopic;
        private final TopicPartition rawPartition;
        private final String targetTopic;
        private final String sourceDatabase;
        private final String sourceTable;
        private final List<String> keyFields;
        private final List<String> sourceColumns;
        private final boolean persistTaskMetrics;
        private final KafkaOutputFormat outputFormat;
        private final KafkaRawRecordReader reader;
        private final AtomicBoolean running = new AtomicBoolean(true);
        /** Released by {@link #close()}: cuts a retry wait short. */
        private final CountDownLatch closed = new CountDownLatch(1);
        private volatile Consumer<String, String> consumer;
        private volatile Future<?> future;
        /**
         * A trailing DELETE held back until the next poll: the CREATE half of an UPDATE split in two may
         * follow it. Its offset stays uncommitted, so a new consumer reads it again.
         */
        private Entry pending;
        /** The raw offset last committed (-1 = none yet), and the registry's copy of it. */
        private long committed = -1;
        private long savedOffset = -1;
        private long savedAtNanos;
        /** Why publishing stopped and since when; null while the bridge publishes. */
        private String stopReason;
        private LocalDateTime stoppedSince;
        /** Raw records still behind the stop (null = not measured), recorded and named in the reason. */
        private Long stopBacklog;
        /** Something was committed in the current consumer session: the retry wait starts over. */
        private boolean progressed;

        private Worker(Long taskId, String ownerType, Long targetSourceId, String bootstrapServers, String rawTopic,
                       String targetTopic, String sourceTable,
                       String sourceDatabase,
                       ZoneId sourceZone,
                       List<String> keyFields,
                       List<String> sourceColumns,
                       boolean persistTaskMetrics,
                       KafkaOutputFormat outputFormat) {
            this.taskId = taskId;
            this.ownerType = ownerType;
            this.targetSourceId = targetSourceId;
            this.bootstrapServers = bootstrapServers;
            this.rawTopic = rawTopic;
            this.rawPartition = new TopicPartition(rawTopic, 0);
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
            int failures = 0;
            try {
                while (running.get()) {
                    progressed = false;
                    stopBacklog = null;
                    String reason;
                    try {
                        reason = consume();
                    } catch (Exception ex) {
                        if (!running.get()) return;
                        reason = STOPPED_PREFIX + KafkaEventProducer.describe(ex);
                        if (!reason.equals(stopReason)) log.error("Kafka task bridge failed: taskId={}", taskId, ex);
                    }
                    if (reason == null || !running.get()) return;
                    failures = progressed ? 1 : failures + 1;
                    Duration wait = backoff(failures);
                    reportStop(reason, wait);
                    if (closed.await(wait.toMillis(), TimeUnit.MILLISECONDS)) return;
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } finally {
                consumer = null;
                running.set(false);
            }
        }

        private boolean isRunning() {
            return running.get() && future != null && !future.isDone();
        }

        /** One consumer session: null once the worker is closed, else why publishing stopped. */
        private String consume() {
            long topicLimit = targetTopicMaxMessageBytes(bootstrapServers, targetTopic);
            Properties config = new Properties();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup(taskId));
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            // Never a silent jump: without a valid committed offset the worker positions itself (position()).
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
            // ensureTopics created the topic before this worker started; a worker still subscribed
            // after the janitor deleted it must not bring it back on a broker that auto-creates topics.
            config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            try (Consumer<String, String> opened = openConsumer(config)) {
                consumer = opened;
                if (!running.get()) return null;
                opened.subscribe(List.of(rawTopic));
                while (running.get()) {
                    ConsumerRecords<String, String> records;
                    try {
                        records = opened.poll(POLL_TIMEOUT);
                    } catch (NoOffsetForPartitionException ex) {
                        String gap = position(opened, ex.partitions(), Map.of());
                        if (gap != null) return gap;
                        continue;
                    } catch (OffsetOutOfRangeException ex) {
                        String gap = position(opened, ex.partitions(), ex.offsetOutOfRangePartitions());
                        if (gap != null) return gap;
                        continue;
                    }
                    String failure = records.isEmpty() ? flushPending(opened, topicLimit) : process(opened, records, topicLimit);
                    if (failure != null) {
                        stopBacklog = backlog(opened);
                        return failure;
                    }
                }
                return null;
            } catch (WakeupException ex) {
                if (running.get()) throw ex;
                return null;
            } finally {
                consumer = null;
                pending = null;
                if (committed > savedOffset) saveOffset(committed, true);
            }
        }

        /**
         * Kafka has no valid position on the raw topic: the group never committed one, its offsets
         * expired (an owner paused longer than the broker's offsets.retention.minutes), retention
         * deleted the records at the committed offset, or that offset lies past the end of a recreated
         * topic. Seeks - and commits - where the bridge may go on and returns null, or returns why it
         * may not: never a silent jump over events it did not publish.
         */
        private String position(Consumer<String, String> opened, Set<TopicPartition> partitions, Map<TopicPartition, Long> refused) {
            KafkaBridgeStateStore.RawPosition recorded = stateStore.rawPosition(targetSourceId, rawTopic);
            for (TopicPartition partition : partitions) {
                long begin = opened.beginningOffsets(List.of(partition)).get(partition);
                long end = opened.endOffsets(List.of(partition)).get(partition);
                Long known = refused.containsKey(partition) ? refused.get(partition) : recorded.offset();
                long target;
                if (known != null && known >= begin && known <= end) {
                    // Group offsets expired but the registry knows the place (or retention moved meanwhile).
                    target = known;
                } else if (known == null && begin == 0) {
                    // Never consumed, and nothing has been deleted yet.
                    target = 0;
                } else if (recorded.resetAllowed()) {
                    // A fresh run: its new initial load supersedes whatever the old position held.
                    target = begin;
                    log.warn("kafka bridge of owner {} starts its fresh run at the earliest retained offset {} of {} (old position {})",
                        taskId, begin, rawTopic, known);
                } else {
                    return gapReason(known, begin, end);
                }
                opened.seek(partition, target);
                opened.commitSync(Map.of(partition, new OffsetAndMetadata(target)));
                committed = target;
                saveOffset(target, true);
                log.info("kafka bridge of owner {} positioned at offset {} of {}", taskId, target, rawTopic);
            }
            if (recorded.resetAllowed()) stateStore.consumeReset(targetSourceId, rawTopic);
            if (isRawTopicGap(stopReason)) recover();
            return null;
        }

        private String gapReason(Long known, long begin, long end) {
            if (known == null) {
                return RAW_TOPIC_GAP_PREFIX + "Kafka 与平台都没有桥接在 " + rawTopic + " 上的消费位点，而它前 " + begin
                    + " 条记录已被保留策略删除，无法确认没有遗漏" + REINITIALIZE_HINT;
            }
            if (known < begin) {
                return RAW_TOPIC_GAP_PREFIX + "桥接停在 " + rawTopic + " 的 offset " + known + "，但它最早只保留到 offset " + begin
                    + "，其间 " + (begin - known) + " 条变更已被 Kafka 保留策略删除、无法补发" + REINITIALIZE_HINT;
            }
            return RAW_TOPIC_GAP_PREFIX + "桥接停在 " + rawTopic + " 的 offset " + known + "，超出它的末尾 offset " + end
                + "：raw topic 可能被删除后重建，期间的变更已丢失" + REINITIALIZE_HINT;
        }

        /**
         * Publishes one poll's records. Returns null with everything published committed, or why
         * publishing stopped, with the raw offset committed up to the event that could not go out.
         */
        private String process(Consumer<String, String> opened, ConsumerRecords<String, String> records, long topicLimit) {
            List<Entry> entries = new ArrayList<>(records.count() + 1);
            if (pending != null) entries.add(pending);
            pending = null;
            long next = -1;
            String unreadable = null;
            for (ConsumerRecord<String, String> record : records) {
                if (!running.get()) return null;
                if (record.partition() != rawPartition.partition()) {
                    return STOPPED_PREFIX + "raw topic " + rawTopic + " 有多个分区，桥接无法保证顺序；请在任务停止时删除该 topic 后重新启动";
                }
                JsonNode event;
                try {
                    event = reader.read(record.value());
                } catch (RuntimeException ex) {
                    unreadable = STOPPED_PREFIX + "停在 raw offset " + record.offset() + "，raw 记录无法解析："
                        + KafkaEventProducer.describe(ex);
                    next = record.offset();
                    break;
                }
                if (event != null) entries.add(new Entry(record.offset(), event));
                next = record.offset() + 1;
            }
            long commitTo = next;
            // MySQL CDC can emit an UPDATE as a DELETE and a CREATE in separate poll batches; a
            // trailing DELETE waits for the next one to give the normalizer a chance to merge it.
            Entry held = null;
            if (!entries.isEmpty() && "d".equals(entries.getLast().event().path("op").asText())) {
                held = entries.removeLast();
                commitTo = held.offset();
            }
            String failure = publish(opened, entries, topicLimit);
            if (failure != null) return failure;
            commit(opened, commitTo);
            if (unreadable != null) return unreadable;
            pending = held;
            if (!entries.isEmpty()) recover();
            return null;
        }

        /** An empty poll releases a held DELETE (a standalone one); an idle bridge that is caught up is no longer stopped. */
        private String flushPending(Consumer<String, String> opened, long topicLimit) {
            if (pending == null) {
                if (stopReason != null && caughtUp(opened)) recover();
                return null;
            }
            Entry held = pending;
            pending = null;
            String failure = publish(opened, List.of(held), topicLimit);
            if (failure != null) return failure;
            commit(opened, held.offset() + 1);
            recover();
            return null;
        }

        /**
         * Normalizes and publishes {@code entries} in order. On a failure, commits up to the raw record
         * of the event that could not be published (everything before it went out) and returns why.
         */
        private String publish(Consumer<String, String> opened, List<Entry> entries, long topicLimit) {
            if (entries.isEmpty()) return null;
            List<KafkaEventNormalizer.Indexed> normalized;
            try {
                normalized = normalize(entries.stream().map(Entry::event).toList());
            } catch (RuntimeException ex) {
                commit(opened, entries.getFirst().offset());
                return STOPPED_PREFIX + "停在 raw offset " + entries.getFirst().offset() + " 起的一批记录，无法转换为事件："
                    + KafkaEventProducer.describe(ex);
            }
            List<KafkaEventNormalizer.NormalizedEvent> events = normalized.stream().map(KafkaEventNormalizer.Indexed::event).toList();
            KafkaEventProducer.PublishOutcome outcome = persistTaskMetrics
                ? producer.publishForTask(taskId, bootstrapServers, targetTopic, events, outputFormat, topicLimit)
                : producer.publish(bootstrapServers, targetTopic, events, outputFormat, topicLimit);
            if (!outcome.failed()) return null;
            Entry failedAt = entries.get(normalized.get(outcome.failedIndex()).input());
            commit(opened, failedAt.offset());
            return STOPPED_PREFIX + "停在 raw offset " + failedAt.offset() + "，" + outcome.failure();
        }

        /**
         * MySQL-CDC (FULL_CDC / INCREMENTAL) records carry an {@code op}; the engine writes Debezium's own
         * records (compatible_debezium_json), so an initial-load row is op=r and published as SNAPSHOT.
         * One engine run reads the binlog only after its whole initial load; a restarted or reinitialized
         * job starts a new initial load in the same raw topic. A job started before that format
         * (SeaTunnel's DEBEZIUM_JSON) wrote its initial load as op=c, indistinguishable from a binlog
         * insert, and that stays CDC. A FULL job's bounded snapshot rows carry no {@code op}. See
         * docs/kafka-event-formats.md.
         */
        private List<KafkaEventNormalizer.Indexed> normalize(List<JsonNode> rawEvents) {
            boolean snapshotRows = rawEvents.stream().allMatch(event -> event != null && !event.has("op"));
            if (snapshotRows) {
                String database = StringUtils.isBlank(sourceDatabase) ? "unknown" : sourceDatabase;
                List<KafkaEventNormalizer.NormalizedEvent> rows = normalizer.normalizeSnapshotRows(
                    rawEvents, database, sourceTable, keyFields, sourceColumns);
                return IntStream.range(0, rows.size()).mapToObj(index -> new KafkaEventNormalizer.Indexed(rows.get(index), index)).toList();
            }
            int snapshotCount = 0;
            while (snapshotCount < rawEvents.size()
                && "r".equals(rawEvents.get(snapshotCount).path("op").asText())) snapshotCount++;
            return normalizer.normalizeIndexed(rawEvents, snapshotCount, keyFields, sourceColumns);
        }

        private void commit(Consumer<String, String> opened, long offset) {
            if (offset < 0 || offset <= committed) return;
            opened.commitSync(Map.of(rawPartition, new OffsetAndMetadata(offset)));
            committed = offset;
            progressed = true;
            saveOffset(offset, false);
        }

        private void saveOffset(long offset, boolean force) {
            long now = System.nanoTime();
            if (!force && savedOffset >= 0 && now - savedAtNanos < OFFSET_SAVE_INTERVAL_NANOS) return;
            stateStore.saveOffset(targetSourceId, rawTopic, offset);
            savedOffset = offset;
            savedAtNanos = now;
        }

        private boolean caughtUp(Consumer<String, String> opened) {
            try {
                if (!opened.assignment().contains(rawPartition)) return false;
                Long end = opened.endOffsets(List.of(rawPartition)).get(rawPartition);
                return end != null && opened.position(rawPartition) >= end;
            } catch (RuntimeException ex) {
                return false;
            }
        }

        /** Raw records from the committed offset to the end of the raw topic, null when Kafka cannot say. */
        private Long backlog(Consumer<String, String> opened) {
            try {
                Long end = opened.endOffsets(List.of(rawPartition)).get(rawPartition);
                return end != null && committed >= 0 ? Math.max(0, end - committed) : null;
            } catch (RuntimeException ex) {
                return null;
            }
        }

        private Duration backoff(int failures) {
            long initial = Math.max(1, properties.getRetryInitialBackoff().toMillis());
            long max = Math.max(initial, properties.getRetryMaxBackoff().toMillis());
            long wait = initial << Math.min(20, Math.max(0, failures - 1));
            return Duration.ofMillis(Math.min(max, wait));
        }

        /** Records why publishing stopped - every attempt, so a record cleared by an operator start comes back. */
        private void reportStop(String reason, Duration wait) {
            if (stoppedSince == null) stoppedSince = LocalDateTime.now();
            if (reason.equals(stopReason)) {
                log.info("kafka bridge of owner {} still stopped, next attempt in {}s", taskId, wait.toSeconds());
            } else {
                log.warn("kafka bridge of owner {} stopped publishing, next attempt in {}s: {}", taskId, wait.toSeconds(), reason);
            }
            stopReason = reason;
            String backlogNote = stopBacklog == null || stopBacklog == 0 ? "" : "（raw topic 积压 " + stopBacklog + " 条）";
            stateStore.reportBlocked(ownerType, taskId, SyncText.truncateForColumn(reason + backlogNote), stoppedSince, stopBacklog);
        }

        private void recover() {
            if (stopReason == null) return;
            log.info("kafka bridge of owner {} publishes again (stopped since {})", taskId, stoppedSince);
            stopReason = null;
            stoppedSince = null;
            stateStore.clearBlocked(ownerType, taskId);
        }

        /**
         * Stop cooperatively first: {@code wakeup()} makes the blocked {@code poll()} /
         * {@code commitSync()} throw, the loop exits and try-with-resources closes the
         * consumer cleanly (leaving the group without a rebalance timeout); a retry wait ends at
         * once. Interrupting straight away, as this used to, made the Kafka client log an ERROR
         * with a stack trace on every ordinary pause / stop. The interrupt is kept only as a last
         * resort for a worker stuck in a producer ack wait, which {@code wakeup()} cannot unblock.
         */
        private void close() {
            running.set(false);
            closed.countDown();
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
