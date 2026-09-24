package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.engine.EngineJobRunner;
import org.dromara.sync.engine.SeaTunnelJobConfigGenerator;
import org.dromara.sync.engine.SourceColumns;
import org.dromara.sync.engine.SyncTaskGroupConfigGenerator;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableSchemaSnapshot;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The steps on one table item of a task group that the group services share: the lifecycle
 * ({@link SyncTaskGroupServiceImpl}), whole-database discovery ({@link SyncTaskGroupDiscoveryServiceImpl})
 * and the row-count check ({@link SyncTaskGroupDataCheckServiceImpl}). Each is a rule about a single
 * table - where its source schema is read from, how its column selection is derived from that schema,
 * what a table the platform picked itself must satisfy, how its one engine job is submitted, how it is
 * parked - decided here once, whichever path the table arrives by. Iterating the tables, locking,
 * refusals and the group's aggregate status stay with the services.
 *
 * <p>Package-private: these are building blocks the group services call under the group lock, not
 * operations for anyone else to run on an item.
 */
@RequiredArgsConstructor
@Component
class GroupItemOperations {

    private final SyncTaskGroupItemMapper itemMapper;
    private final IDataSourceMetadataService metadataService;
    private final SeaTunnelProperties properties;
    private final EngineJobRunner runner;

    // ------------------------------------------------------------------ source schema & selection

    /** The live structure of the item's source table. */
    DataSourceMetadataVo readSourceMetadata(SyncTaskGroupItem item, DataSource source) {
        return metadataService.queryTableMetadata(source.getSourceId(), sourceDatabase(item, source), item.getSourceTable());
    }

    /** The item's own source database, else the data source's default one. */
    static String sourceDatabase(SyncTaskGroupItem item, DataSource source) {
        return StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName());
    }

    /** Expands / validates the column projection and sync key against live source metadata. */
    static void applySelection(SyncTaskGroupItem item, DataSourceMetadataVo metadata) {
        SyncColumnSelectionValidator.Selection selection = SyncColumnSelectionValidator.validate(metadata,
            item.getSelectedColumns(), item.getSyncKeyColumns());
        item.setSelectedColumns(SyncColumnSelectionValidator.serialize(selection.selectedColumns()));
        item.setSyncKeyColumns(SyncColumnSelectionValidator.serialize(selection.syncKeyColumns()));
    }

    /**
     * A selection that covered every column of the previous baseline means "the whole table",
     * so it is re-derived from the live schema and picks up columns added since; an explicit
     * subset is left alone. The sync key is never re-chosen. Returns true when columns were added.
     */
    static boolean followSourceColumns(SyncTaskGroupItem item, DataSourceMetadataVo metadata) {
        List<String> configured = SyncColumnSelectionValidator.parseColumns(item.getSelectedColumns());
        TableSchemaSnapshot.Snapshot baseline = StringUtils.isBlank(item.getSchemaSnapshot()) ? null : TableSchemaSnapshot.fromJson(item.getSchemaSnapshot());
        if (!TableSchemaSnapshot.coversAllColumns(baseline, configured)) return false;
        SyncColumnSelectionValidator.Selection selection = SyncColumnSelectionValidator.validate(metadata, null, item.getSyncKeyColumns());
        item.setSelectedColumns(SyncColumnSelectionValidator.serialize(selection.selectedColumns()));
        item.setSyncKeyColumns(SyncColumnSelectionValidator.serialize(selection.syncKeyColumns()));
        return selection.selectedColumns().size() > configured.size();
    }

    /**
     * Null when a table of a whole-database group - picked by discovery, not by the operator - has
     * a usable sync key and a compatible target, otherwise the reason. Discovery inserts a table
     * that fails this as FAILED; an edit of the group and a whole-database Kafka start re-admit one
     * that now passes.
     */
    String validateDiscoveredItem(DataSource source, DataSource target, SyncTaskGroupItem item) {
        try {
            DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(), item.getSourceDatabase(), item.getSourceTable());
            boolean hasKey = !metadata.getPrimaryKeys().isEmpty()
                || metadata.getUniqueKeys().stream().anyMatch(key -> Boolean.TRUE.equals(key.getAllNotNull()));
            if (!hasKey) return "源表没有可用同步键";
            TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
                item.getSourceTable(), item.getTargetSchema(), item.getTargetTable());
            return compatibility.isPassed() ? null : compatibility.getMessage();
        } catch (RuntimeException ex) {
            return StringUtils.defaultIfBlank(ex.getMessage(), "新增表校验失败");
        }
    }

    // ------------------------------------------------------------------ the item's engine job

    /**
     * Submits one table item as its own SeaTunnel job. Returns the engine job id.
     * <p>For Kafka targets {@link EngineJobRunner} starts the bridge first, as on the single-task
     * path: its topic precheck and capacity check refuse before any engine job exists, and it
     * creates the single-partition raw topic before the engine can auto-create it with broker
     * defaults. A failed submit tears the bridge down again.
     */
    String submit(SyncTaskGroup group, SyncTaskGroupItem item, DataSource source, DataSource target) {
        var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties, sourceColumns(source));
        SeaTunnelJobConfigGenerator.prepareTarget(SyncTaskGroupConfigGenerator.toTask(group, item), source, target, generated, sourceColumns(source));
        String jobId = runner.submit(job(group, item, source, target), generated);
        item.setEngineJobId(jobId);
        item.setEngineConfigHash(generated.fingerprint());
        item.setStatus(SyncStatus.RUNNING);
        item.setLastError("");
        itemMapper.updateById(item);
        return jobId;
    }

    /** One table item as the engine job runner sees it: projected onto a task, with the group's endpoints. */
    static EngineJobRunner.Job job(SyncTaskGroup group, SyncTaskGroupItem item, DataSource source, DataSource target) {
        return new EngineJobRunner.Job(SyncTaskGroupConfigGenerator.toTask(group, item), source, target, true);
    }

    /** Parks the table FAILED with the reason. Its job id stays, so a retry can still reach that job. */
    void isolate(SyncTaskGroupItem item, String error) {
        item.setStatus(SyncStatus.FAILED);
        item.setLastError(SyncText.truncateForColumn(error));
        itemMapper.updateById(item);
    }

    private SourceColumns sourceColumns(DataSource source) {
        return SourceColumns.fromMetadata(metadataService, source);
    }
}
