package org.dromara.sync.service;

import org.dromara.sync.domain.vo.DataSourceCdcPrecheckVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** JDBC metadata and CDC prerequisite checks for task creation. */
public interface IDataSourceMetadataService {

    /**
     * Outcome of one table inside a batch read: exactly one of the two components is set, so a
     * single unreadable table (dropped, or permission revoked) does not cost the others their read.
     */
    record TableMetadata(DataSourceMetadataVo metadata, String error) {
    }

    List<String> queryDatabases(Long sourceId);

    List<String> queryTables(Long sourceId, String databaseName);

    DataSourceMetadataVo queryTableMetadata(Long sourceId, String databaseName, String tableName);

    DataSourceMetadataVo queryTableMetadata(Long sourceId, String databaseName, String schemaName, String tableName);

    /**
     * Reads several tables of one database over a <em>single</em> connection, keyed by the trimmed
     * table name. The DDL check walks every table of every live group once a minute, and
     * {@link org.dromara.sync.support.JdbcUrls#open} is unpooled — one connection per table meant a
     * 20-table group opened 20 TCP + auth handshakes a minute against the customer's source
     * database, which sits badly with the source-protection the platform otherwise promises.
     */
    Map<String, TableMetadata> queryTablesMetadata(Long sourceId, String databaseName, Collection<String> tableNames);

    DataSourceCdcPrecheckVo checkMysqlCdc(Long sourceId);

    TargetCompatibilityVo checkTargetCompatibility(Long taskId);

    TargetCompatibilityVo checkTargetCompatibility(Long sourceId, Long targetId, String sourceTable,
                                                    String targetSchema, String targetTable);

    TargetCompatibilityVo checkTargetCompatibility(Long sourceId, Long targetId, String sourceTable,
                                                    String targetSchema, String targetTable,
                                                    String selectedColumns, String syncKeyColumns);
}
