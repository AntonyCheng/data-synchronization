package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dromara.sync.config.SyncSchedulingConfig;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.engine.SyncTaskGroupConfigGenerator;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Keeps this process's Kafka bridge workers equal to what the database says should be
 * bridged: a worker exists for every RUNNING Kafka-target task and every RUNNING item
 * of a live Kafka-target group, and for nothing else.
 *
 * <p>Runs on every backend instance. Because the private raw topic has a single
 * partition, all instances joining a task's consumer group means exactly one of them
 * consumes while the others stand by - a crashed instance's work moves to a survivor
 * on the next rebalance without any platform coordination. The same pass retires
 * workers left behind when a task was stopped, paused or reinitialized from another
 * instance (previously those lingered as idle consumers until a restart).
 *
 * <p>Failures to start a bridge (typically a missing output topic) are logged once per
 * failing owner and again when it recovers, not on every pass.
 *
 * <p>A full bridge pool ({@code sync.kafka-bridge.max-workers}) is not a failure here: every
 * owner this pass starts already runs on the engine, so the bridge parks it (logging the
 * transition once) and the pass leaves its status alone. The capacity check precedes any
 * AdminClient or consumer, so asking again every pass is free, and the bridge resumes from
 * its committed offset in the raw topic as soon as a slot frees.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaBridgeReconciler {

    private final SyncTaskMapper syncTaskMapper;
    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final DataSourceMapper dataSourceMapper;
    private final KafkaTaskBridgeService bridge;

    /** Owners whose bridge failed to start on the previous pass, to keep the log quiet while it persists. */
    private final Set<Long> failing = new HashSet<>();

    @Scheduled(fixedDelayString = "${sync.kafka-bridge.reconcile-interval-ms:30000}", initialDelayString = "${sync.kafka-bridge.reconcile-initial-delay-ms:40000}", scheduler = SyncSchedulingConfig.SCHEDULER)
    public void reconcile() {
        try {
            reconcileOnce();
        } catch (RuntimeException ex) {
            log.warn("kafka bridge reconcile pass failed: {}", ex.getMessage());
        }
    }

    /** One pass; package-private for tests. Returns the number of workers started + stopped. */
    int reconcileOnce() {
        Map<Long, Desired> desired = desiredBridges();
        int changed = 0;
        for (Long ownerId : bridge.localOwnerIds()) {
            if (desired.containsKey(ownerId)) continue;
            bridge.stop(ownerId);
            failing.remove(ownerId);
            log.info("kafka bridge stopped: owner {} is no longer running", ownerId);
            changed++;
        }
        for (Desired want : desired.values()) {
            if (bridge.isRunning(want.ownerId())) continue;
            try {
                boolean started = want.groupItem()
                    ? bridge.tryStartGroupItem(want.task(), want.target(), want.source())
                    : bridge.tryStart(want.task(), want.target(), want.source());
                if (!started) continue;
                if (failing.remove(want.ownerId())) log.info("kafka bridge recovered: owner {}", want.ownerId());
                else log.info("kafka bridge started: owner {} ({})", want.ownerId(), want.groupItem() ? "group item" : "task");
                changed++;
            } catch (RuntimeException ex) {
                if (failing.add(want.ownerId())) {
                    log.warn("kafka bridge could not be started for owner {}: {}", want.ownerId(), ex.getMessage());
                }
            }
        }
        return changed;
    }

    private Map<Long, Desired> desiredBridges() {
        Map<Long, DataSource> dataSources = new HashMap<>();
        Map<Long, Desired> desired = new LinkedHashMap<>();
        for (SyncTask task : syncTaskMapper.selectActive()) {
            if (!SyncStatus.RUNNING.equals(task.getStatus())) continue;
            DataSource target = dataSource(dataSources, task.getTargetId());
            if (!DataSourceType.isKafka(target)) continue;
            DataSource source = dataSource(dataSources, task.getSourceId());
            desired.put(task.getTaskId(), new Desired(task.getTaskId(), task, target, source, false));
        }
        for (SyncTaskGroup group : groupMapper.selectLive()) {
            DataSource target = dataSource(dataSources, group.getTargetId());
            if (!DataSourceType.isKafka(target)) continue;
            DataSource source = dataSource(dataSources, group.getSourceId());
            for (SyncTaskGroupItem item : itemMapper.selectByGroupId(group.getGroupId())) {
                if (!SyncStatus.RUNNING.equals(item.getStatus())) continue;
                desired.put(item.getItemId(), new Desired(item.getItemId(),
                    SyncTaskGroupConfigGenerator.toTask(group, item), target, source, true));
            }
        }
        return desired;
    }

    private DataSource dataSource(Map<Long, DataSource> cache, Long id) {
        if (id == null) return null;
        return cache.computeIfAbsent(id, dataSourceMapper::selectById);
    }

    private record Desired(Long ownerId, SyncTask task, DataSource target, DataSource source, boolean groupItem) {
    }
}
