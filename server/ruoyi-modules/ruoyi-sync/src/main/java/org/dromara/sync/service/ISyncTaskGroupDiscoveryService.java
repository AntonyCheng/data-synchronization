package org.dromara.sync.service;

import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;

import java.util.List;

/**
 * New-table discovery for whole-database task groups: the platform, not the operator, decides
 * which tables such a group syncs, so every table of its source database that has no table item
 * yet gets one - and, on a live group, its own engine job right away.
 *
 * <p>Discovery comes in two halves. {@link #plan} and {@link #readmitRejected} do the remote work -
 * list the source database, read each table's schema, create a whole-database Kafka group's topics,
 * check the sync key and the target - and write nothing, so a save runs them before its transaction
 * opens and then writes the planned rows in one short transaction. {@link #discover} is the pass on
 * an existing group: it plans, then inserts the rows and submits the new tables' jobs.
 */
public interface ISyncTaskGroupDiscoveryService {

    /** Discover tables of a database-scope group and isolate new table failures. */
    SyncTaskGroupOperationResult discover(Long groupId);

    /**
     * The tables of the group's source database that are not among {@code present}, each as a
     * prepared item (not inserted; its group id is the group's, null for a group not yet saved):
     * column selection and schema baseline from the live source, its topic created on a Kafka
     * target, and FAILED with the reason when it fails the discovery rules. Fills only the room
     * {@code present} leaves under {@code sync.group.max-tables}. Reads the source database and
     * Kafka; writes nothing. Throws when the source database cannot be listed.
     *
     * @param present the items the group has, or will keep: their tables are not discovered again,
     *                they count toward the table limit, and new tables follow their target schema
     */
    TablePlan plan(SyncTaskGroup group, List<SyncTaskGroupItem> present);

    /**
     * Re-checks against the discovery rules every item that was rejected before it ever ran -
     * FAILED with no engine job: no usable sync key, an incompatible target, a topic that could not
     * be created, a refused submit - and returns the items it changed: PENDING again when they now
     * pass, otherwise still FAILED with the current reason. A whole-database start skips FAILED
     * tables, so without this such a table would stay out of the group for good.
     * Reads the source database and Kafka; changes only the given objects, writes nothing.
     */
    List<SyncTaskGroupItem> readmitRejected(SyncTaskGroup group, List<SyncTaskGroupItem> items);

    /**
     * What {@link #plan} found: one item per new table in source order - PENDING, or FAILED with the
     * reason (inserted all the same so the operator sees it, but never submitted) - and how many
     * tables the table limit left out.
     */
    record TablePlan(List<SyncTaskGroupItem> items, int overLimit, int maxTables) {

        /** New tables that failed the discovery rules. */
        public int rejected() {
            return (int) items.stream().filter(item -> SyncStatus.FAILED.equals(item.getStatus())).count();
        }

        /** Says how many tables the limit left out; empty when it left none out. */
        public String limitNote() {
            return overLimit == 0 ? ""
                : "已达任务组上限 " + maxTables + " 张，另有 " + overLimit + " 张表未纳入（sync.group.max-tables）";
        }

        /** The group's error note after discovery: how many tables failed, then the limit note. */
        public String note(long failed) {
            return join(failed == 0 ? "" : "新增表发现完成，其中 " + failed + " 张表校验或提交失败，请查看表项错误", limitNote());
        }

        /** Two notes joined, either of which may be empty. */
        public static String join(String first, String second) {
            if (StringUtils.isBlank(first)) return second;
            if (StringUtils.isBlank(second)) return first;
            return first + "；" + second;
        }
    }
}
