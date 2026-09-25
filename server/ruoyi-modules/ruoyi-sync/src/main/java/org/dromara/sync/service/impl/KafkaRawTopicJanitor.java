package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.config.SyncSchedulingConfig;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaRawTopic;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.engine.SyncTaskGroupConfigGenerator;
import org.dromara.sync.kafka.KafkaAdminClients;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.KafkaRawTopicMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncText;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Deletes the private raw topics ({@code __ds_raw_{ownerId}_v{configVersion}}) that no task or
 * table item uses any more. Without it every deleted owner, every edit (a new config version is a
 * new raw topic) and every target switch left a topic on the customer's cluster for good, holding
 * whole source rows - including the columns the operator deselected.
 *
 * <p>Only topics registered in {@code ds_kafka_raw_topic} are candidates; the bridge registers the
 * raw topic of every owner it starts. Another platform environment may share the cluster and name
 * its raw topics the same way, so a topic is never deleted because of its name.
 *
 * <p>A registered topic is <em>live</em> while its owner exists, still targets that data source and
 * still maps to that topic name ({@link KafkaTaskBridgeService#rawTopic}). Comparing the version is
 * safe because it only changes on an edit, and an owner can only be edited while its job is
 * stopped; a stopped owner started again with the same version reuses the same topic. A topic that
 * stops being live is first marked retired and deleted only after
 * {@code sync.kafka-bridge.raw-topic-retire-grace}: a bridge worker on another instance may still
 * be draining it until that instance's reconciler retires the worker. When the owner itself is
 * gone, its bridge consumer group is deleted with the topic.
 *
 * <p>A deleted topic's row is kept for another grace period so that a topic re-created by a
 * lingering client is deleted again; the bridge's own consumers never auto-create it.
 *
 * <p>One instance at a time: the pass runs under a Redisson lock taken without waiting, and a
 * busy pass is skipped. Each data source is handled on its own - one AdminClient and one topic
 * listing per pass - so an unreachable cluster keeps its rows for the next pass and never holds up
 * the others. The only rows written are the registry's.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaRawTopicJanitor {

    private static final long ADMIN_TIMEOUT_SECONDS = 10;
    /** Owners are loaded with IN lists of at most this many ids. */
    private static final int ID_BATCH = 500;

    private final KafkaRawTopicMapper rawTopicMapper;
    private final SyncTaskMapper taskMapper;
    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final DataSourceMapper dataSourceMapper;
    private final KafkaBridgeProperties properties;
    private final SyncLocks locks;

    @Scheduled(fixedDelayString = "${sync.kafka-bridge.raw-topic-cleanup-interval-ms:300000}", initialDelayString = "${sync.kafka-bridge.raw-topic-cleanup-initial-delay-ms:60000}", scheduler = SyncSchedulingConfig.SCHEDULER)
    public void clean() {
        try {
            if (!cleanExclusively(LocalDateTime.now())) log.debug("kafka raw topic cleanup skipped: another instance runs it");
        } catch (RuntimeException ex) {
            log.warn("kafka raw topic cleanup pass failed: {}", ex.getMessage());
        }
    }

    /** One pass unless another instance holds the lock ({@code false}); package-private for tests. */
    boolean cleanExclusively(LocalDateTime now) {
        return locks.runIfFree(SyncLocks.RAW_TOPIC_CLEANUP_LOCK, () -> cleanOnce(now));
    }

    /** One pass; package-private for tests. Returns the number of topics deleted from a cluster. */
    int cleanOnce(LocalDateTime now) {
        List<KafkaRawTopic> rows = rawTopicMapper.selectList();
        if (rows.isEmpty()) return 0;
        // Rows before owners: a row registered after this read is simply seen next pass.
        Owners owners = loadOwners(rows);
        Map<Long, List<KafkaRawTopic>> byDataSource = rows.stream()
            .collect(Collectors.groupingBy(KafkaRawTopic::getDataSourceId, LinkedHashMap::new, Collectors.toList()));
        Map<Long, DataSource> dataSources = byId(byDataSource.keySet(), dataSourceMapper::selectByIds, DataSource::getSourceId);
        int deleted = 0;
        for (Map.Entry<Long, List<KafkaRawTopic>> entry : byDataSource.entrySet()) {
            try {
                deleted += cleanDataSource(entry.getKey(), dataSources.get(entry.getKey()), entry.getValue(), owners, now);
            } catch (RuntimeException ex) {
                log.warn("kafka raw topic cleanup of data source {} failed, retried next pass: {}", entry.getKey(), ex.getMessage());
            }
        }
        return deleted;
    }

    private int cleanDataSource(Long dataSourceId, DataSource target, List<KafkaRawTopic> rows, Owners owners, LocalDateTime now) {
        if (!DataSourceType.isKafka(target)) {
            // Nothing to connect to any more, so nothing this registry could ever clean up.
            rawTopicMapper.deleteByDataSource(dataSourceId);
            log.warn("data source {} is {}: dropped its {} registered raw topic(s) from the cleanup, "
                    + "they stay on that cluster: {}", dataSourceId, target == null ? "deleted" : "no longer a Kafka data source",
                rows.size(), rows.stream().map(KafkaRawTopic::getTopicName).toList());
            return 0;
        }
        LocalDateTime graceStart = now.minus(grace());
        List<KafkaRawTopic> due = new ArrayList<>();
        List<KafkaRawTopic> deleted = new ArrayList<>();
        for (KafkaRawTopic row : rows) {
            if (!recognized(row)) continue;
            if (owners.isLive(row)) {
                // The deleted mark too: a topic in use must never be deleted as a re-created one.
                if (row.getRetiredTime() != null || row.getDeletedTime() != null) {
                    rawTopicMapper.markInUse(row.getRawTopicId());
                    log.info("raw topic {} is in use again by {} {}", row.getTopicName(), row.getOwnerType(), row.getOwnerId());
                }
            } else if (row.getDeletedTime() != null) {
                deleted.add(row);
            } else if (row.getRetiredTime() == null) {
                rawTopicMapper.markRetired(row.getRawTopicId(), now);
                log.info("raw topic {} retired: {} {} {}, deleted after {}", row.getTopicName(), row.getOwnerType(),
                    row.getOwnerId(), owners.isGone(row) ? "no longer exists" : "no longer uses it", grace());
            } else if (row.getRetiredTime().isBefore(graceStart)) {
                due.add(row);
            }
        }
        if (due.isEmpty() && deleted.isEmpty()) return 0;
        try (AdminClient admin = openAdmin(target)) {
            return purge(admin, dataSourceId, due, deleted, owners, now, graceStart);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            log.warn("kafka raw topic cleanup of data source {} failed (cluster unreachable?), {} topic(s) kept for the next pass: {}",
                dataSourceId, due.size() + deleted.size(), SyncText.safeMessage(ex, ex.getClass().getSimpleName()));
        }
        return 0;
    }

    /**
     * Deletes the topics retired past the grace and those that re-appeared after their deletion,
     * and drops the rows of deleted topics that stayed away for the grace. A topic missing from the
     * listing counts as deleted. Per-topic failures keep their rows and are logged in one WARN.
     */
    private int purge(AdminClient admin, Long dataSourceId, List<KafkaRawTopic> due, List<KafkaRawTopic> deleted,
                      Owners owners, LocalDateTime now, LocalDateTime graceStart) throws Exception {
        Set<String> existing = admin.listTopics().names().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        List<KafkaRawTopic> reappeared = deleted.stream().filter(row -> existing.contains(row.getTopicName())).toList();
        List<String> present = new ArrayList<>();
        due.stream().map(KafkaRawTopic::getTopicName).filter(existing::contains).forEach(present::add);
        reappeared.stream().map(KafkaRawTopic::getTopicName).forEach(present::add);
        Map<String, KafkaFuture<Void>> deletions = present.isEmpty() ? Map.of() : admin.deleteTopics(present).topicNameValues();
        Map<String, Throwable> failures = new LinkedHashMap<>();
        Set<String> orphanedGroups = new LinkedHashSet<>();
        int count = 0;
        for (KafkaRawTopic row : due) {
            if (!awaitGone(row.getTopicName(), deletions.get(row.getTopicName()), failures)) continue;
            rawTopicMapper.markDeleted(row.getRawTopicId(), now);
            if (existing.contains(row.getTopicName())) count++;
            log.info("raw topic {} deleted from data source {} (retired since {})", row.getTopicName(), dataSourceId, row.getRetiredTime());
            if (owners.isGone(row)) orphanedGroups.add(KafkaTaskBridgeService.consumerGroup(row.getOwnerId()));
        }
        for (KafkaRawTopic row : reappeared) {
            if (!awaitGone(row.getTopicName(), deletions.get(row.getTopicName()), failures)) continue;
            rawTopicMapper.markDeleted(row.getRawTopicId(), now);
            count++;
            log.warn("raw topic {} on data source {} was re-created after its deletion (a client still uses it) and was deleted again",
                row.getTopicName(), dataSourceId);
        }
        for (KafkaRawTopic row : deleted) {
            if (!existing.contains(row.getTopicName()) && row.getDeletedTime().isBefore(graceStart)) {
                rawTopicMapper.deleteById(row.getRawTopicId());
            }
        }
        deleteConsumerGroups(admin, orphanedGroups, failures);
        if (!failures.isEmpty()) {
            Throwable first = failures.values().iterator().next();
            log.warn("kafka raw topic cleanup on data source {}: could not delete {}, retried next pass: {}", dataSourceId,
                failures.keySet(), SyncText.safeMessage(first, first.getClass().getSimpleName()));
        }
        return count;
    }

    /** A gone owner's consumer group; one that is already gone or still has a member is left alone. */
    private void deleteConsumerGroups(AdminClient admin, Set<String> groups, Map<String, Throwable> failures)
        throws InterruptedException {
        if (groups.isEmpty()) return;
        Map<String, KafkaFuture<Void>> results = admin.deleteConsumerGroups(groups).deletedGroups();
        for (String group : groups) {
            KafkaFuture<Void> result = results.get(group);
            if (result == null) continue;
            try {
                result.get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                log.info("kafka bridge consumer group {} deleted: its owner no longer exists", group);
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof GroupIdNotFoundException) {
                    log.debug("kafka bridge consumer group {} was already gone", group);
                } else if (ex.getCause() instanceof GroupNotEmptyException) {
                    log.info("kafka bridge consumer group {} still has a member and was kept; Kafka expires it once empty", group);
                } else {
                    failures.put(group, ex.getCause() == null ? ex : ex.getCause());
                }
            } catch (TimeoutException ex) {
                failures.put(group, ex);
            }
        }
    }

    /** True once the topic is gone: deleted now, already missing, or not listed at all. */
    private static boolean awaitGone(String topic, KafkaFuture<Void> deletion, Map<String, Throwable> failures)
        throws InterruptedException {
        if (deletion == null) return true;
        try {
            deletion.get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof UnknownTopicOrPartitionException) return true;
            failures.put(topic, ex.getCause() == null ? ex : ex.getCause());
        } catch (TimeoutException ex) {
            failures.put(topic, ex);
        }
        return false;
    }

    /** Rows the bridge would have written; anything else (edited by hand) is left alone rather than risk a wrong delete. */
    private static boolean recognized(KafkaRawTopic row) {
        return row.getOwnerId() != null && row.getTopicName() != null
            && row.getTopicName().startsWith(KafkaTaskBridgeService.RAW_TOPIC_PREFIX)
            && (KafkaRawTopic.OWNER_TASK.equals(row.getOwnerType()) || KafkaRawTopic.OWNER_GROUP_ITEM.equals(row.getOwnerType()));
    }

    private Duration grace() {
        Duration grace = properties.getRawTopicRetireGrace();
        return grace == null || grace.isNegative() ? Duration.ZERO : grace;
    }

    private Owners loadOwners(List<KafkaRawTopic> rows) {
        Map<Long, SyncTask> tasks = byId(ownerIds(rows, KafkaRawTopic.OWNER_TASK), taskMapper::selectByIds, SyncTask::getTaskId);
        Map<Long, SyncTaskGroupItem> items = byId(ownerIds(rows, KafkaRawTopic.OWNER_GROUP_ITEM), itemMapper::selectByIds,
            SyncTaskGroupItem::getItemId);
        Set<Long> groupIds = items.values().stream().map(SyncTaskGroupItem::getGroupId).filter(Objects::nonNull)
            .collect(Collectors.toSet());
        return new Owners(tasks, items, byId(groupIds, groupMapper::selectByIds, SyncTaskGroup::getGroupId));
    }

    private static Set<Long> ownerIds(List<KafkaRawTopic> rows, String ownerType) {
        return rows.stream().filter(row -> ownerType.equals(row.getOwnerType())).map(KafkaRawTopic::getOwnerId)
            .filter(Objects::nonNull).collect(Collectors.toSet());
    }

    private static <T> Map<Long, T> byId(Collection<Long> ids, Function<Collection<Long>, List<T>> select, Function<T, Long> idOf) {
        Map<Long, T> found = new HashMap<>();
        List<Long> all = List.copyOf(ids);
        for (int from = 0; from < all.size(); from += ID_BATCH) {
            for (T row : select.apply(all.subList(from, Math.min(all.size(), from + ID_BATCH)))) found.put(idOf.apply(row), row);
        }
        return found;
    }

    /** Package-private for tests. */
    AdminClient openAdmin(DataSource target) {
        return KafkaAdminClients.open(target);
    }

    /** The owners of the registered topics as they are now, loaded once per pass. */
    private record Owners(Map<Long, SyncTask> tasks, Map<Long, SyncTaskGroupItem> items, Map<Long, SyncTaskGroup> groups) {

        /** The owner the way the bridge would start it now; null once it no longer exists. */
        private SyncTask current(KafkaRawTopic row) {
            if (KafkaRawTopic.OWNER_TASK.equals(row.getOwnerType())) return tasks.get(row.getOwnerId());
            SyncTaskGroupItem item = items.get(row.getOwnerId());
            SyncTaskGroup group = item == null ? null : groups.get(item.getGroupId());
            return group == null ? null : SyncTaskGroupConfigGenerator.toTask(group, item);
        }

        private boolean isLive(KafkaRawTopic row) {
            SyncTask owner = current(row);
            return owner != null && Objects.equals(owner.getTargetId(), row.getDataSourceId())
                && KafkaTaskBridgeService.rawTopic(owner).equals(row.getTopicName());
        }

        private boolean isGone(KafkaRawTopic row) {
            return current(row) == null;
        }
    }
}
