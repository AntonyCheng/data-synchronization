package org.dromara.sync.service;

import org.dromara.common.core.domain.PageResult;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.domain.bo.DataSourceBo;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceCredentialMigrationResult;
import org.dromara.sync.domain.vo.DataSourceVo;

/**
 * Data source service.
 */
public interface IDataSourceService {

    PageResult<DataSourceVo> queryPageList(DataSourceBo bo, PageQuery pageQuery);

    DataSourceVo queryById(Long sourceId);

    Boolean insertByBo(DataSourceBo bo);

    Boolean updateByBo(DataSourceBo bo);

    Boolean deleteById(Long sourceId);

    ConnectionTestResult testConnection(Long sourceId, DataSourceBo request);

    DataSourceCredentialMigrationResult migrateCredentials();
}
