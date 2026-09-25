package org.dromara.sync.service.impl;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaRawTopic;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.KafkaRawTopicMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.support.SyncLocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The raw-topic janitor against an in-memory registry and fake Kafka clusters (one per data
 * source); only rows of {@code ds_kafka_raw_topic} are ever written.
 */
@Tag("dev")
class KafkaRawTopicJanitorTest {

    private static final long KAFKA = 10L;
    private static final long OTHER_KAFKA = 11L;
    private static final LocalDateTime NOW = LocalDateTime.of(2026, 9, 25, 12, 0);

    private final KafkaRawTopicMapper rawTopicMapper = mock(KafkaRawTopicMapper.class);
    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final DataSourceMapper dataSourceMapper = mock(DataSourceMapper.class);
    private final RedissonClient redisson = mock(RedissonClient.class);
    private final RLock lock = mock(RLock.class);

    private final List<KafkaRawTopic> registry = new ArrayList<>();
    private final Map<Long, SyncTask> tasks = new HashMap<>();
    private final Map<Long, SyncTaskGroup> groups = new HashMap<>();
    private final Map<Long, SyncTaskGroupItem> items = new HashMap<>();
    private final Map<Long, DataSource> dataSources = new HashMap<>();
    private final Map<Long, Cluster> clusters = new HashMap<>();
    private long nextRowId = 1;

    private final KafkaRawTopicJanitor janitor = new KafkaRawTopicJanitor(rawTopicMapper, taskMapper, groupMapper, itemMapper,
        dataSourceMapper, new KafkaBridgeProperties(), new SyncLocks(redisson)) {
        @Override
        AdminClient openAdmin(DataSource target) {
            return clusters.get(target.getSourceId()).open();
        }
    };

    @BeforeEach
    void world() throws Exception {
        when(redisson.getLock(SyncLocks.RAW_TOPIC_CLEANUP_LOCK)).thenReturn(lock);
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        when(rawTopicMapper.selectList()).thenAnswer(inv -> registry.stream().map(KafkaRawTopicJanitorTest::copy).toList());
        when(rawTopicMapper.markRetired(anyLong(), any())).thenAnswer(inv -> update(inv.getArgument(0),
            row -> row.setRetiredTime(inv.getArgument(1))));
        when(rawTopicMapper.markDeleted(anyLong(), any())).thenAnswer(inv -> update(inv.getArgument(0),
            row -> row.setDeletedTime(inv.getArgument(1))));
        when(rawTopicMapper.markInUse(anyLong())).thenAnswer(inv -> update(inv.getArgument(0), row -> {
            row.setRetiredTime(null);
            row.setDeletedTime(null);
        }));
        when(rawTopicMapper.deleteById(any(Serializable.class))).thenAnswer(inv ->
            registry.removeIf(row -> row.getRawTopicId().equals(inv.getArgument(0))) ? 1 : 0);
        when(rawTopicMapper.deleteByDataSource(anyLong())).thenAnswer(inv ->
            registry.removeIf(row -> row.getDataSourceId().equals(inv.getArgument(0))) ? 1 : 0);

        when(taskMapper.selectByIds(anyCollection())).thenAnswer(inv -> pick(tasks, inv.getArgument(0)));
        when(groupMapper.selectByIds(anyCollection())).thenAnswer(inv -> pick(groups, inv.getArgument(0)));
        when(itemMapper.selectByIds(anyCollection())).thenAnswer(inv -> pick(items, inv.getArgument(0)));
        when(dataSourceMapper.selectByIds(anyCollection())).thenAnswer(inv -> pick(dataSources, inv.getArgument(0)));

        kafka(KAFKA);
        kafka(OTHER_KAFKA);
    }

    @Test
    void aTopicItsOwnerStillUsesIsLeftAlone() {
        task(1L, KAFKA, 2);
        KafkaRawTopic row = register(KAFKA, "__ds_raw_1_v2", KafkaRawTopic.OWNER_TASK, 1L);

        assertEquals(0, janitor.cleanOnce(NOW));

        assertNull(row(row).getRetiredTime());
        assertTrue(clusters.get(KAFKA).topics.contains("__ds_raw_1_v2"));
        assertEquals(0, clusters.get(KAFKA).opened, "nothing to delete, so no AdminClient");
    }

    @Test
    void aSupersededVersionIsRetiredFirstAndDeletedOnlyAfterTheGrace() {
        task(1L, KAFKA, 3);
        register(KAFKA, "__ds_raw_1_v3", KafkaRawTopic.OWNER_TASK, 1L);
        KafkaRawTopic old = register(KAFKA, "__ds_raw_1_v2", KafkaRawTopic.OWNER_TASK, 1L);
        clusters.get(KAFKA).groups.add("ds-task-1-bridge");

        janitor.cleanOnce(NOW);
        assertEquals(NOW, row(old).getRetiredTime());
        assertTrue(clusters.get(KAFKA).topics.contains("__ds_raw_1_v2"), "another instance's worker may still drain it");

        assertEquals(0, janitor.cleanOnce(NOW.plusMinutes(9)));
        assertTrue(clusters.get(KAFKA).topics.contains("__ds_raw_1_v2"));

        assertEquals(1, janitor.cleanOnce(NOW.plusMinutes(11)));
        assertFalse(clusters.get(KAFKA).topics.contains("__ds_raw_1_v2"));
        assertEquals(NOW.plusMinutes(11), row(old).getDeletedTime());
        assertTrue(clusters.get(KAFKA).topics.contains("__ds_raw_1_v3"), "the live version stays");
        assertTrue(clusters.get(KAFKA).groups.contains("ds-task-1-bridge"), "the owner still reads v3 with its group");
    }

    @Test
    void aDeletedOwnerLosesItsTopicAndItsConsumerGroupButUnregisteredTopicsAreNeverTouched() {
        KafkaRawTopic gone = register(KAFKA, "__ds_raw_5_v1", KafkaRawTopic.OWNER_TASK, 5L);
        Cluster cluster = clusters.get(KAFKA);
        cluster.groups.add("ds-task-5-bridge");
        // Same naming scheme, same cluster, but another environment's: not in the registry.
        cluster.topics.add("__ds_raw_6_v1");
        cluster.groups.add("ds-task-6-bridge");

        janitor.cleanOnce(NOW);
        assertEquals(1, janitor.cleanOnce(NOW.plusMinutes(11)));

        assertFalse(cluster.topics.contains("__ds_raw_5_v1"));
        assertFalse(cluster.groups.contains("ds-task-5-bridge"));
        assertNotNull(row(gone).getDeletedTime());
        assertTrue(cluster.topics.contains("__ds_raw_6_v1"));
        assertTrue(cluster.groups.contains("ds-task-6-bridge"));
        assertEquals(List.of("__ds_raw_5_v1"), cluster.deletedTopics);
    }

    @Test
    void aMissingConsumerGroupIsNotAnError() {
        KafkaRawTopic gone = register(KAFKA, "__ds_raw_5_v1", KafkaRawTopic.OWNER_TASK, 5L);
        row(gone).setRetiredTime(NOW.minusMinutes(11));

        assertEquals(1, janitor.cleanOnce(NOW));

        assertEquals(NOW, row(gone).getDeletedTime());
    }

    @Test
    void aTopicLeftOnTheOldTargetAfterASwitchIsDeleted() {
        task(1L, OTHER_KAFKA, 2);
        KafkaRawTopic before = register(KAFKA, "__ds_raw_1_v2", KafkaRawTopic.OWNER_TASK, 1L);
        KafkaRawTopic current = register(OTHER_KAFKA, "__ds_raw_1_v2", KafkaRawTopic.OWNER_TASK, 1L);
        row(before).setRetiredTime(NOW.minusMinutes(11));
        clusters.get(KAFKA).groups.add("ds-task-1-bridge");
        clusters.get(OTHER_KAFKA).groups.add("ds-task-1-bridge");

        assertEquals(1, janitor.cleanOnce(NOW));

        assertFalse(clusters.get(KAFKA).topics.contains("__ds_raw_1_v2"));
        assertTrue(clusters.get(OTHER_KAFKA).topics.contains("__ds_raw_1_v2"));
        assertNull(row(current).getRetiredTime());
        assertFalse(clusters.get(KAFKA).groups.contains("ds-task-1-bridge"), "nothing reads with it on the old cluster any more");
        assertTrue(clusters.get(OTHER_KAFKA).groups.contains("ds-task-1-bridge"), "the owner reads the new cluster with it");
    }

    @Test
    void aGroupItemsTopicFollowsTheGroupsVersionAndTarget() {
        group(7L, KAFKA, 4);
        item(70L, 7L);
        KafkaRawTopic live = register(KAFKA, "__ds_raw_70_v4", KafkaRawTopic.OWNER_GROUP_ITEM, 70L);
        KafkaRawTopic superseded = register(KAFKA, "__ds_raw_70_v3", KafkaRawTopic.OWNER_GROUP_ITEM, 70L);
        KafkaRawTopic removedTable = register(KAFKA, "__ds_raw_71_v4", KafkaRawTopic.OWNER_GROUP_ITEM, 71L);
        row(superseded).setRetiredTime(NOW.minusMinutes(11));
        row(removedTable).setRetiredTime(NOW.minusMinutes(11));
        Cluster cluster = clusters.get(KAFKA);
        cluster.groups.add("ds-task-70-bridge");
        cluster.groups.add("ds-task-71-bridge");

        assertEquals(2, janitor.cleanOnce(NOW));

        assertNull(row(live).getRetiredTime());
        assertTrue(cluster.topics.contains("__ds_raw_70_v4"));
        assertFalse(cluster.topics.contains("__ds_raw_70_v3"));
        assertFalse(cluster.topics.contains("__ds_raw_71_v4"));
        assertTrue(cluster.groups.contains("ds-task-70-bridge"), "the table still runs");
        assertFalse(cluster.groups.contains("ds-task-71-bridge"), "the table is gone");
    }

    @Test
    void aTopicInUseAgainIsNeverDeleted() {
        task(1L, KAFKA, 2);
        KafkaRawTopic row = register(KAFKA, "__ds_raw_1_v2", KafkaRawTopic.OWNER_TASK, 1L);
        row(row).setRetiredTime(NOW.minusMinutes(30));
        row(row).setDeletedTime(NOW.minusMinutes(1));

        assertEquals(0, janitor.cleanOnce(NOW));

        assertNull(row(row).getRetiredTime());
        assertNull(row(row).getDeletedTime());
        assertTrue(clusters.get(KAFKA).topics.contains("__ds_raw_1_v2"));
    }

    @Test
    void aTopicReCreatedAfterItsDeletionIsDeletedAgain() {
        KafkaRawTopic row = register(KAFKA, "__ds_raw_5_v1", KafkaRawTopic.OWNER_TASK, 5L);
        row(row).setRetiredTime(NOW.minusMinutes(20));
        row(row).setDeletedTime(NOW.minusMinutes(2));

        assertEquals(1, janitor.cleanOnce(NOW));

        assertFalse(clusters.get(KAFKA).topics.contains("__ds_raw_5_v1"));
        assertEquals(NOW, row(row).getDeletedTime(), "watched for another grace period");
    }

    @Test
    void theRowOfADeletedTopicIsDroppedOnceTheTopicStayedAwayForTheGrace() {
        KafkaRawTopic recent = register(KAFKA, "__ds_raw_5_v1", KafkaRawTopic.OWNER_TASK, 5L);
        KafkaRawTopic settled = register(KAFKA, "__ds_raw_5_v2", KafkaRawTopic.OWNER_TASK, 5L);
        clusters.get(KAFKA).topics.clear();
        row(recent).setRetiredTime(NOW.minusMinutes(20));
        row(recent).setDeletedTime(NOW.minusMinutes(2));
        row(settled).setRetiredTime(NOW.minusMinutes(30));
        row(settled).setDeletedTime(NOW.minusMinutes(11));

        assertEquals(0, janitor.cleanOnce(NOW));

        assertEquals(List.of("__ds_raw_5_v1"), registry.stream().map(KafkaRawTopic::getTopicName).toList());
        assertTrue(clusters.get(KAFKA).deletedTopics.isEmpty());
    }

    @Test
    void rowsOfAMissingOrNoLongerKafkaDataSourceAreForgotten() {
        task(1L, KAFKA, 1);
        register(99L, "__ds_raw_1_v1", KafkaRawTopic.OWNER_TASK, 1L);
        DataSource mysql = new DataSource();
        mysql.setSourceId(12L);
        mysql.setSourceType("MYSQL");
        dataSources.put(12L, mysql);
        register(12L, "__ds_raw_2_v1", KafkaRawTopic.OWNER_TASK, 2L);
        register(KAFKA, "__ds_raw_1_v1", KafkaRawTopic.OWNER_TASK, 1L);

        janitor.cleanOnce(NOW);

        assertEquals(List.of(KAFKA), registry.stream().map(KafkaRawTopic::getDataSourceId).toList());
    }

    @Test
    void anUnreachableClusterKeepsItsRowsAndDoesNotHoldUpTheOthers() {
        KafkaRawTopic stuck = register(KAFKA, "__ds_raw_5_v1", KafkaRawTopic.OWNER_TASK, 5L);
        KafkaRawTopic other = register(OTHER_KAFKA, "__ds_raw_8_v1", KafkaRawTopic.OWNER_TASK, 8L);
        row(stuck).setRetiredTime(NOW.minusMinutes(11));
        row(other).setRetiredTime(NOW.minusMinutes(11));
        clusters.get(KAFKA).unreachable = true;

        assertEquals(1, janitor.cleanOnce(NOW));

        assertNull(row(stuck).getDeletedTime());
        assertTrue(clusters.get(KAFKA).topics.contains("__ds_raw_5_v1"));
        assertEquals(NOW, row(other).getDeletedTime());
        assertFalse(clusters.get(OTHER_KAFKA).topics.contains("__ds_raw_8_v1"));

        clusters.get(KAFKA).unreachable = false;
        assertEquals(1, janitor.cleanOnce(NOW.plusMinutes(5)));
        assertEquals(NOW.plusMinutes(5), row(stuck).getDeletedTime());
    }

    @Test
    void aPassIsSkippedWhileAnotherInstanceRunsIt() throws Exception {
        register(KAFKA, "__ds_raw_5_v1", KafkaRawTopic.OWNER_TASK, 5L);
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(false);

        assertFalse(janitor.cleanExclusively(NOW));

        verifyNoInteractions(rawTopicMapper, taskMapper, dataSourceMapper);
        assertEquals(0, clusters.get(KAFKA).opened);
    }

    @Test
    void aPassRunsUnderTheLock() {
        register(KAFKA, "__ds_raw_5_v1", KafkaRawTopic.OWNER_TASK, 5L);

        assertTrue(janitor.cleanExclusively(NOW));

        assertEquals(NOW, registry.getFirst().getRetiredTime());
    }

    private void kafka(long id) {
        DataSource kafka = new DataSource();
        kafka.setSourceId(id);
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka-" + id);
        kafka.setPort(9092);
        dataSources.put(id, kafka);
        clusters.put(id, new Cluster());
    }

    private void task(long id, long targetId, int version) {
        SyncTask task = new SyncTask();
        task.setTaskId(id);
        task.setTargetId(targetId);
        task.setConfigVersion(version);
        tasks.put(id, task);
    }

    private void group(long id, long targetId, int version) {
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(id);
        group.setTargetId(targetId);
        group.setConfigVersion(version);
        groups.put(id, group);
    }

    private void item(long id, long groupId) {
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(id);
        item.setGroupId(groupId);
        item.setSourceTable("t" + id);
        items.put(id, item);
    }

    /** Registers the topic and puts it on the data source's cluster, as a bridge start would. */
    private KafkaRawTopic register(long dataSourceId, String topic, String ownerType, long ownerId) {
        KafkaRawTopic row = new KafkaRawTopic();
        row.setRawTopicId(nextRowId++);
        row.setDataSourceId(dataSourceId);
        row.setTopicName(topic);
        row.setOwnerType(ownerType);
        row.setOwnerId(ownerId);
        row.setConfigVersion(Integer.valueOf(topic.substring(topic.lastIndexOf("_v") + 2)));
        row.setCreateTime(NOW.minusDays(1));
        registry.add(row);
        Cluster cluster = clusters.get(dataSourceId);
        if (cluster != null) cluster.topics.add(topic);
        return row;
    }

    /** The stored row (what the janitor wrote), not a copy it read. */
    private KafkaRawTopic row(KafkaRawTopic registered) {
        return registry.stream().filter(row -> row.getRawTopicId().equals(registered.getRawTopicId())).findFirst().orElseThrow();
    }

    private int update(Long rawTopicId, Consumer<KafkaRawTopic> change) {
        registry.stream().filter(row -> row.getRawTopicId().equals(rawTopicId)).forEach(change);
        return 1;
    }

    private static <T> List<T> pick(Map<Long, T> table, Collection<?> ids) {
        return ids.stream().map(table::get).filter(Objects::nonNull).toList();
    }

    private static KafkaRawTopic copy(KafkaRawTopic row) {
        KafkaRawTopic copy = new KafkaRawTopic();
        copy.setRawTopicId(row.getRawTopicId());
        copy.setDataSourceId(row.getDataSourceId());
        copy.setTopicName(row.getTopicName());
        copy.setOwnerType(row.getOwnerType());
        copy.setOwnerId(row.getOwnerId());
        copy.setConfigVersion(row.getConfigVersion());
        copy.setRetiredTime(row.getRetiredTime());
        copy.setDeletedTime(row.getDeletedTime());
        copy.setCreateTime(row.getCreateTime());
        return copy;
    }

    private static <T> KafkaFuture<T> failed(Throwable error) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        return future;
    }

    /** A Kafka cluster reduced to its topic and consumer group names. */
    private static final class Cluster {
        private final Set<String> topics = new HashSet<>();
        private final Set<String> groups = new HashSet<>();
        private final List<String> deletedTopics = new ArrayList<>();
        private boolean unreachable;
        private int opened;

        private AdminClient open() {
            opened++;
            AdminClient admin = mock(AdminClient.class);
            ListTopicsResult listing = mock(ListTopicsResult.class);
            when(listing.names()).thenAnswer(inv -> unreachable
                ? failed(new TimeoutException("Timed out waiting for a node assignment. Call: listTopics"))
                : KafkaFuture.completedFuture(Set.copyOf(topics)));
            when(admin.listTopics()).thenReturn(listing);
            when(admin.deleteTopics(anyCollection())).thenAnswer(inv -> {
                Map<String, KafkaFuture<Void>> results = results(inv.getArgument(0), name -> {
                    if (!topics.remove(name)) return failed(new UnknownTopicOrPartitionException(name));
                    deletedTopics.add(name);
                    return KafkaFuture.completedFuture(null);
                });
                DeleteTopicsResult deleted = mock(DeleteTopicsResult.class);
                when(deleted.topicNameValues()).thenReturn(results);
                return deleted;
            });
            when(admin.deleteConsumerGroups(anyCollection())).thenAnswer(inv -> {
                Map<String, KafkaFuture<Void>> results = results(inv.getArgument(0), name -> groups.remove(name)
                    ? KafkaFuture.completedFuture(null)
                    : failed(new GroupIdNotFoundException("The group id does not exist.")));
                DeleteConsumerGroupsResult deleted = mock(DeleteConsumerGroupsResult.class);
                when(deleted.deletedGroups()).thenReturn(results);
                return deleted;
            });
            return admin;
        }

        private static Map<String, KafkaFuture<Void>> results(Collection<String> names, Function<String, KafkaFuture<Void>> outcome) {
            Map<String, KafkaFuture<Void>> results = new HashMap<>();
            for (String name : names) results.put(name, outcome.apply(name));
            return results;
        }
    }
}
