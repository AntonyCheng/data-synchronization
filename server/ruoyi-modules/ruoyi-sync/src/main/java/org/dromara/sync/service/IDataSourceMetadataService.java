package org.dromara.sync.service;

import org.dromara.sync.domain.vo.DataSourceCdcPrecheckVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;

import java.util.List;

/** JDBC metadata and CDC prerequisite checks for task creation. */
public interface IDataSourceMetadataService {

    List<String> queryDatabases(Long sourceId);

    List<String> queryTables(Long sourceId, String databaseName);

    DataSourceMetadataVo queryTableMetadata(Long sourceId, String databaseName, String tableName);

    DataSourceMetadataVo queryTableMetadata(Long sourceId, String databaseName, String schemaName, String tableName);

    DataSourceCdcPrecheckVo checkMysqlCdc(Long sourceId);

    TargetCompatibilityVo checkTargetCompatibility(Long taskId);

    TargetCompatibilityVo checkTargetCompatibility(Long sourceId, Long targetId, String sourceTable,
                                                    String targetSchema, String targetTable);

    TargetCompatibilityVo checkTargetCompatibility(Long sourceId, Long targetId, String sourceTable,
                                                    String targetSchema, String targetTable,
                                                    String selectedColumns, String syncKeyColumns);
}
