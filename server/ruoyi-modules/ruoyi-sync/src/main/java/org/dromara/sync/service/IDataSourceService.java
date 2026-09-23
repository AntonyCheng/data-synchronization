package org.dromara.sync.service;

import org.dromara.common.core.domain.PageResult;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.bo.DataSourceBo;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceCredentialMigrationResult;
import org.dromara.sync.domain.vo.DataSourceOptionVo;
import org.dromara.sync.domain.vo.DataSourceVo;

import java.util.List;

/**
 * Data source service.
 */
public interface IDataSourceService {

    PageResult<DataSourceVo> queryPageList(DataSourceBo bo, PageQuery pageQuery);

    DataSourceVo queryById(Long sourceId);

    /**
     * Every data source, id + name + type + database only. The wizards need the full list to
     * populate their source / target pickers; paging them (the pages used to ask for the first
     * 100) silently hides the rest.
     */
    List<DataSourceOptionVo> options();

    Boolean insertByBo(DataSourceBo bo);

    Boolean updateByBo(DataSourceBo bo);

    Boolean deleteById(Long sourceId);

    ConnectionTestResult testConnection(Long sourceId, DataSourceBo request);

    DataSourceCredentialMigrationResult migrateCredentials();

    /**
     * Loads the entity (credential included) for in-process use by the other sync services.
     * Fails with a {@code ServiceException} naming {@code side} ("源" / "目标") when the id is
     * missing or unknown.
     */
    DataSource requireById(Long sourceId, String side);

    /**
     * Like {@link #requireById} but additionally rejects a relational data source without a
     * configured password - the engine and the JDBC checks could not connect with it anyway.
     * Kafka brokers are commonly unauthenticated and are exempt.
     */
    DataSource requireUsable(Long sourceId, String side);
}
