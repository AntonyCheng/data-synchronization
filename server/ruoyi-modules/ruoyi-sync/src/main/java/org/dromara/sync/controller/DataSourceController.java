package org.dromara.sync.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.domain.PageResult;
import org.dromara.common.core.domain.R;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.common.log.annotation.Log;
import org.dromara.common.log.enums.BusinessType;
import org.dromara.common.redis.annotation.RepeatSubmit;
import org.dromara.common.web.core.BaseController;
import org.dromara.sync.domain.bo.DataSourceBo;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceCdcPrecheckVo;
import org.dromara.sync.domain.vo.DataSourceCredentialMigrationResult;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.DataSourceVo;
import org.dromara.sync.domain.bo.KafkaTopicCreateBo;
import org.dromara.sync.domain.vo.KafkaTopicVo;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Data source management API.
 */
@Validated
@RequiredArgsConstructor
@RestController
@RequestMapping("/sync/data-source")
public class DataSourceController extends BaseController {

    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;

    @SaCheckPermission("sync:data-source:list")
    @GetMapping("/list")
    public R<PageResult<DataSourceVo>> list(DataSourceBo bo, PageQuery pageQuery) {
        return R.ok(dataSourceService.queryPageList(bo, pageQuery));
    }

    @SaCheckPermission("sync:data-source:query")
    @GetMapping("/{sourceId}")
    public R<DataSourceVo> getInfo(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId) {
        return R.ok(dataSourceService.queryById(sourceId));
    }

    @SaCheckPermission("sync:data-source:add")
    @Log(title = "同步数据源", businessType = BusinessType.INSERT)
    @RepeatSubmit()
    @PostMapping
    public R<Void> add(@Validated @RequestBody DataSourceBo bo) {
        return toAjax(dataSourceService.insertByBo(bo));
    }

    @SaCheckPermission("sync:data-source:edit")
    @Log(title = "同步数据源", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PutMapping
    public R<Void> edit(@Validated @RequestBody DataSourceBo bo) {
        return toAjax(dataSourceService.updateByBo(bo));
    }

    @SaCheckPermission("sync:data-source:remove")
    @Log(title = "同步数据源", businessType = BusinessType.DELETE)
    @DeleteMapping("/{sourceId}")
    public R<Void> remove(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId) {
        return toAjax(dataSourceService.deleteById(sourceId));
    }

    @SaCheckPermission("sync:data-source:test")
    @Log(title = "同步数据源连接测试", businessType = BusinessType.OTHER)
    @PostMapping("/{sourceId}/test")
    public R<ConnectionTestResult> test(@PathVariable Long sourceId, @RequestBody(required = false) DataSourceBo bo) {
        return R.ok(dataSourceService.testConnection(sourceId, bo));
    }

    @SaCheckPermission("sync:data-source:metadata")
    @GetMapping("/{sourceId}/databases")
    public R<List<String>> databases(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId) {
        return R.ok(metadataService.queryDatabases(sourceId));
    }

    @SaCheckPermission("sync:data-source:metadata")
    @GetMapping("/{sourceId}/tables")
    public R<List<String>> tables(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId, String databaseName) {
        return R.ok(metadataService.queryTables(sourceId, databaseName));
    }

    @SaCheckPermission("sync:data-source:metadata")
    @GetMapping("/{sourceId}/metadata")
    public R<DataSourceMetadataVo> metadata(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId,
                                            String databaseName, @NotNull(message = "表名不能为空") String tableName) {
        return R.ok(metadataService.queryTableMetadata(sourceId, databaseName, tableName));
    }

    @SaCheckPermission("sync:data-source:cdc-precheck")
    @PostMapping("/{sourceId}/cdc-precheck")
    public R<DataSourceCdcPrecheckVo> cdcPrecheck(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId) {
        return R.ok(metadataService.checkMysqlCdc(sourceId));
    }

    @SaCheckPermission("sync:data-source:metadata")
    @GetMapping("/{sourceId}/kafka/topics")
    public R<List<KafkaTopicVo>> kafkaTopics(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId) {
        return R.ok(metadataService.listKafkaTopics(sourceId));
    }

    @SaCheckPermission("sync:data-source:metadata")
    @Log(title = "创建 Kafka topic", businessType = BusinessType.INSERT)
    @RepeatSubmit()
    @PostMapping("/{sourceId}/kafka/topics")
    public R<KafkaTopicVo> createKafkaTopic(@NotNull(message = "数据源ID不能为空") @PathVariable Long sourceId,
                                            @Validated @RequestBody KafkaTopicCreateBo bo) {
        return R.ok(metadataService.createKafkaTopic(sourceId, bo));
    }

    @SaCheckPermission("sync:data-source:credential-migrate")
    @Log(title = "迁移同步数据源凭证", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PostMapping("/credential-migrate")
    public R<DataSourceCredentialMigrationResult> migrateCredentials() {
        return R.ok(dataSourceService.migrateCredentials());
    }
}
