package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.domain.PageResult;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.MapstructUtils;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.bo.DataSourceBo;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceCredentialMigrationResult;
import org.dromara.sync.domain.vo.DataSourceVo;
import org.dromara.common.encrypt.properties.EncryptorProperties;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.service.IDataSourceService;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.ObjectProvider;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;

/**
 * Data source service implementation.
 */
@RequiredArgsConstructor
@Service
public class DataSourceServiceImpl implements IDataSourceService {

    private static final Set<String> SUPPORTED_TYPES = Set.of("MYSQL", "POSTGRESQL", "KAFKA");
    private final DataSourceMapper dataSourceMapper;
    private final ObjectProvider<EncryptorProperties> encryptorProperties;

    @Override
    public PageResult<DataSourceVo> queryPageList(DataSourceBo bo, PageQuery pageQuery) {
        LambdaQueryWrapper<DataSource> wrapper = new LambdaQueryWrapper<>();
        wrapper.like(StringUtils.isNotBlank(bo.getSourceName()), DataSource::getSourceName, bo.getSourceName())
            .eq(StringUtils.isNotBlank(bo.getSourceType()), DataSource::getSourceType, normalizeType(bo.getSourceType()))
            .eq(StringUtils.isNotBlank(bo.getStatus()), DataSource::getStatus, bo.getStatus())
            .orderByDesc(DataSource::getSourceId);
        Page<DataSourceVo> page = dataSourceMapper.selectVoPage(pageQuery.build(), wrapper);
        return PageResult.build(page.getRecords(), page.getTotal());
    }

    @Override
    public DataSourceVo queryById(Long sourceId) {
        return dataSourceMapper.selectVoById(sourceId);
    }

    @Override
    public Boolean insertByBo(DataSourceBo bo) {
        DataSource entity = MapstructUtils.convert(bo, DataSource.class);
        normalizeAndValidate(entity, true);
        return dataSourceMapper.insert(entity) > 0;
    }

    @Override
    public Boolean updateByBo(DataSourceBo bo) {
        DataSource current = dataSourceMapper.selectById(bo.getSourceId());
        if (current == null) {
            throw new ServiceException("数据源不存在");
        }
        DataSource entity = MapstructUtils.convert(bo, DataSource.class);
        if (StringUtils.isBlank(entity.getPassword())) {
            entity.setPassword(current.getPassword());
        }
        normalizeAndValidate(entity, false);
        return dataSourceMapper.updateById(entity) > 0;
    }

    @Override
    public Boolean deleteById(Long sourceId) {
        return dataSourceMapper.deleteById(sourceId) > 0;
    }

    @Override
    public ConnectionTestResult testConnection(Long sourceId, DataSourceBo request) {
        DataSource entity;
        if (sourceId != null) {
            entity = dataSourceMapper.selectById(sourceId);
            if (entity == null) {
                return ConnectionTestResult.failure("数据源不存在");
            }
            if (request != null) {
                DataSource requested = MapstructUtils.convert(request, DataSource.class);
                mergeConnectionFields(entity, requested);
            }
        } else {
            entity = MapstructUtils.convert(request, DataSource.class);
        }
        try {
            normalizeAndValidate(entity, true);
            Instant started = Instant.now();
            if ("KAFKA".equals(entity.getSourceType())) {
                try (AdminClient admin = AdminClient.create(kafkaProperties(entity))) {
                    admin.describeCluster().nodes().get(5, java.util.concurrent.TimeUnit.SECONDS);
                    return ConnectionTestResult.success(Duration.between(started, Instant.now()).toMillis());
                }
            }
            try (Connection ignored = DriverManager.getConnection(buildJdbcUrl(entity), entity.getUsername(), entity.getPassword())) {
                return ConnectionTestResult.success(Duration.between(started, Instant.now()).toMillis());
            }
        } catch (Exception ex) {
            String message = StringUtils.isBlank(ex.getMessage()) ? "连接失败" : ex.getMessage();
            return ConnectionTestResult.failure(message);
        }
    }

    @Override
    public DataSourceCredentialMigrationResult migrateCredentials() {
        EncryptorProperties properties = encryptorProperties.getIfAvailable();
        if (properties == null || !Boolean.TRUE.equals(properties.getEnable())) {
            throw new ServiceException("请先启用 mybatis-encryptor.enable，并注入 SYNC_CREDENTIAL_ENCRYPTION_PASSWORD");
        }
        DataSourceCredentialMigrationResult result = new DataSourceCredentialMigrationResult();
        var sources = dataSourceMapper.selectList(new LambdaQueryWrapper<>());
        result.setEnabled(true);
        result.setTotal(sources.size());
        int migrated = 0;
        for (DataSource source : sources) {
            if (StringUtils.isNotBlank(source.getPassword())) {
                // The encrypt interceptor is idempotent for ENC_ values and encrypts legacy plaintext here.
                dataSourceMapper.updateById(source);
                migrated++;
            }
        }
        result.setMigrated(migrated);
        result.setMessage("凭证迁移完成，已处理 " + migrated + " 个数据源");
        return result;
    }

    private void mergeConnectionFields(DataSource target, DataSource request) {
        if (request == null) {
            return;
        }
        if (StringUtils.isNotBlank(request.getSourceType())) target.setSourceType(request.getSourceType());
        if (StringUtils.isNotBlank(request.getHost())) target.setHost(request.getHost());
        if (request.getPort() != null) target.setPort(request.getPort());
        if (StringUtils.isNotBlank(request.getDatabaseName())) target.setDatabaseName(request.getDatabaseName());
        if (StringUtils.isNotBlank(request.getSchemaName())) target.setSchemaName(request.getSchemaName());
        if (StringUtils.isNotBlank(request.getUsername())) target.setUsername(request.getUsername());
        if (StringUtils.isNotBlank(request.getPassword())) target.setPassword(request.getPassword());
        if (StringUtils.isNotBlank(request.getSslEnabled())) target.setSslEnabled(request.getSslEnabled());
    }

    private void normalizeAndValidate(DataSource entity, boolean passwordRequired) {
        if (entity == null) {
            throw new ServiceException("数据源参数不能为空");
        }
        entity.setSourceType(normalizeType(entity.getSourceType()));
        if (!SUPPORTED_TYPES.contains(entity.getSourceType())) {
            throw new ServiceException("仅支持 MySQL、PostgreSQL 和 Kafka 数据源");
        }
        boolean kafka = "KAFKA".equals(entity.getSourceType());
        if (StringUtils.isBlank(entity.getHost()) || entity.getPort() == null || (!kafka && (StringUtils.isBlank(entity.getDatabaseName())
            || StringUtils.isBlank(entity.getUsername()) || (passwordRequired && StringUtils.isBlank(entity.getPassword()))))) {
            throw new ServiceException("数据源连接参数不完整");
        }
        if (StringUtils.isBlank(entity.getSslEnabled())) entity.setSslEnabled("0");
        if (StringUtils.isBlank(entity.getStatus())) entity.setStatus("0");
    }

    private String normalizeType(String type) {
        return StringUtils.isBlank(type) ? type : type.trim().toUpperCase(Locale.ROOT);
    }

    private String buildJdbcUrl(DataSource entity) {
        String type = entity.getSourceType();
        if ("MYSQL".equals(type)) {
            return "jdbc:mysql://" + entity.getHost() + ":" + entity.getPort() + "/" + entity.getDatabaseName()
                + "?connectTimeout=5000&socketTimeout=5000&useSSL=" + "1".equals(entity.getSslEnabled())
                + "&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        }
        return "jdbc:postgresql://" + entity.getHost() + ":" + entity.getPort() + "/" + entity.getDatabaseName()
            + "?connectTimeout=5&socketTimeout=5&ssl=" + "1".equals(entity.getSslEnabled());
    }

    private static java.util.Properties kafkaProperties(DataSource entity) {
        java.util.Properties properties = new java.util.Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, entity.getHost() + ':' + entity.getPort());
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        return properties;
    }
}
