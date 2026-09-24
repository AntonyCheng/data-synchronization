package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.dromara.common.core.domain.PageResult;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.MapstructUtils;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.common.encrypt.properties.EncryptorProperties;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.bo.DataSourceBo;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceCredentialMigrationResult;
import org.dromara.sync.domain.vo.DataSourceOptionVo;
import org.dromara.sync.domain.vo.DataSourceVo;
import org.dromara.sync.kafka.KafkaAdminClients;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.support.JdbcUrls;
import org.dromara.sync.support.SourceTimeZones;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Data source service implementation.
 */
@RequiredArgsConstructor
@Service
public class DataSourceServiceImpl implements IDataSourceService {

    private final DataSourceMapper dataSourceMapper;
    private final SyncTaskMapper syncTaskMapper;
    private final SyncTaskGroupMapper syncTaskGroupMapper;
    private final ObjectProvider<EncryptorProperties> encryptorProperties;

    @Override
    public PageResult<DataSourceVo> queryPageList(DataSourceBo bo, PageQuery pageQuery) {
        LambdaQueryWrapper<DataSource> wrapper = new LambdaQueryWrapper<>();
        wrapper.like(StringUtils.isNotBlank(bo.getSourceName()), DataSource::getSourceName, bo.getSourceName())
            .eq(StringUtils.isNotBlank(bo.getSourceType()), DataSource::getSourceType, DataSourceType.normalize(bo.getSourceType()))
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
    public List<DataSourceOptionVo> options() {
        return dataSourceMapper.selectList(new LambdaQueryWrapper<DataSource>()
                .orderByAsc(DataSource::getSourceName))
            .stream()
            .map(source -> {
                DataSourceOptionVo option = new DataSourceOptionVo();
                option.setSourceId(source.getSourceId());
                option.setSourceName(source.getSourceName());
                option.setSourceType(source.getSourceType());
                option.setDatabaseName(source.getDatabaseName());
                option.setStatus(source.getStatus());
                return option;
            })
            .toList();
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
        // A blank password on edit keeps the stored (possibly encrypted) credential.
        if (StringUtils.isBlank(entity.getPassword())) {
            entity.setPassword(current.getPassword());
        }
        // An omitted time zone keeps the stored one; an empty string clears it (compatibility mode).
        if (entity.getServerTimeZone() == null) {
            entity.setServerTimeZone(current.getServerTimeZone());
        }
        normalizeAndValidate(entity, false);
        if (connectionChanged(current, entity)) ensureNotInUse(current.getSourceId());
        return dataSourceMapper.updateById(entity) > 0;
    }

    /**
     * Endpoint / identity fields. A running SeaTunnel job keeps the connection it was submitted
     * with, so silently repointing the data source underneath it would leave the platform
     * describing a job that no longer exists. The server time zone is part of the submitted CDC
     * config too. Name, remark, status and - deliberately - the password (credential rotation)
     * may change at any time; the next start picks them up.
     */
    static boolean connectionChanged(DataSource current, DataSource updated) {
        return !Objects.equals(current.getSourceType(), updated.getSourceType())
            || !Objects.equals(current.getHost(), updated.getHost())
            || !Objects.equals(current.getPort(), updated.getPort())
            || !Objects.equals(StringUtils.defaultIfBlank(current.getDatabaseName(), ""), StringUtils.defaultIfBlank(updated.getDatabaseName(), ""))
            || !Objects.equals(StringUtils.defaultIfBlank(current.getSchemaName(), ""), StringUtils.defaultIfBlank(updated.getSchemaName(), ""))
            || !Objects.equals(StringUtils.defaultIfBlank(current.getUsername(), ""), StringUtils.defaultIfBlank(updated.getUsername(), ""))
            || !Objects.equals(StringUtils.defaultIfBlank(current.getSslEnabled(), "0"), StringUtils.defaultIfBlank(updated.getSslEnabled(), "0"))
            || !Objects.equals(SourceTimeZones.effective(current), SourceTimeZones.effective(updated));
    }

    private void ensureNotInUse(Long sourceId) {
        long tasks = syncTaskMapper.countActiveByDataSource(sourceId);
        long groups = syncTaskGroupMapper.countActiveByDataSource(sourceId);
        if (tasks > 0 || groups > 0) {
            throw new ServiceException("数据源正被 " + tasks + " 个运行中的同步任务、" + groups
                + " 个任务组使用，运行期间不能修改连接参数（类型/主机/端口/库名/schema/用户名/SSL/服务器时区）；请先停止或暂停这些任务，名称、备注和密码可直接修改");
        }
    }

    @Override
    public Boolean deleteById(Long sourceId) {
        long tasks = syncTaskMapper.countByDataSource(sourceId);
        long groups = syncTaskGroupMapper.countByDataSource(sourceId);
        if (tasks > 0 || groups > 0) {
            throw new ServiceException("数据源仍被 " + tasks + " 个同步任务、" + groups + " 个任务组引用，请先删除或改配这些任务");
        }
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
                mergeConnectionFields(entity, MapstructUtils.convert(request, DataSource.class));
            }
        } else {
            entity = MapstructUtils.convert(request, DataSource.class);
        }
        try {
            normalizeAndValidate(entity, true);
            Instant started = Instant.now();
            if (DataSourceType.isKafka(entity)) {
                try (AdminClient admin = KafkaAdminClients.open(entity)) {
                    admin.describeCluster().nodes().get(5, TimeUnit.SECONDS);
                    return ConnectionTestResult.success(Duration.between(started, Instant.now()).toMillis());
                }
            }
            try (Connection connection = JdbcUrls.open(entity)) {
                ConnectionTestResult result = ConnectionTestResult.success(Duration.between(started, Instant.now()).toMillis());
                if (DataSourceType.isMysql(entity)) {
                    Instant now = Instant.now();
                    result.setTimeZone(SourceTimeZones.verdict(entity, SourceTimeZones.read(connection, now), now));
                }
                return result;
            }
        } catch (Exception ex) {
            return ConnectionTestResult.failure(StringUtils.isBlank(ex.getMessage()) ? "连接失败" : ex.getMessage());
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

    @Override
    public DataSource requireById(Long sourceId, String side) {
        if (sourceId == null) throw new ServiceException(side + "数据源不能为空");
        DataSource source = dataSourceMapper.selectById(sourceId);
        if (source == null) throw new ServiceException(side + "数据源不存在");
        return source;
    }

    @Override
    public DataSource requireUsable(Long sourceId, String side) {
        DataSource source = requireById(sourceId, side);
        if (!DataSourceType.isKafka(source) && StringUtils.isBlank(source.getPassword())) {
            throw new ServiceException(side + "数据源密码未配置");
        }
        return source;
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
        // Blank is a real value here (compatibility mode), so only an omitted zone keeps the stored one.
        if (request.getServerTimeZone() != null) target.setServerTimeZone(request.getServerTimeZone());
    }

    private void normalizeAndValidate(DataSource entity, boolean passwordRequired) {
        if (entity == null) {
            throw new ServiceException("数据源参数不能为空");
        }
        entity.setSourceType(DataSourceType.normalize(entity.getSourceType()));
        if (entity.getSourceType() == null || !DataSourceType.ALL.contains(entity.getSourceType())) {
            throw new ServiceException("仅支持 MySQL、PostgreSQL 和 Kafka 数据源");
        }
        boolean kafka = DataSourceType.isKafka(entity);
        if (StringUtils.isBlank(entity.getHost()) || entity.getPort() == null || (!kafka && (StringUtils.isBlank(entity.getDatabaseName())
            || StringUtils.isBlank(entity.getUsername()) || (passwordRequired && StringUtils.isBlank(entity.getPassword()))))) {
            throw new ServiceException("数据源连接参数不完整");
        }
        if (StringUtils.isBlank(entity.getSslEnabled())) entity.setSslEnabled("0");
        if (StringUtils.isBlank(entity.getStatus())) entity.setStatus("0");
        // Only a MySQL source is told a zone; for other types the field has no meaning.
        entity.setServerTimeZone(DataSourceType.isMysql(entity) ? SourceTimeZones.normalize(entity.getServerTimeZone())
            : entity.getServerTimeZone() == null ? null : "");
    }
}
