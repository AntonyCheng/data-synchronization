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
import org.dromara.sync.domain.bo.SyncTaskBo;
import org.dromara.sync.domain.bo.SyncTaskDataCheckRequest;
import org.dromara.sync.domain.vo.SyncTaskValidationResult;
import org.dromara.sync.domain.vo.SyncTaskVo;
import org.dromara.sync.domain.vo.SeaTunnelJobConfigPreview;
import org.dromara.sync.domain.vo.SeaTunnelJobOperationResult;
import org.dromara.sync.domain.vo.SeaTunnelJobStatus;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.ISeaTunnelJobService;
import org.dromara.sync.service.ISyncTaskService;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Synchronization task configuration API.
 */
@Validated
@RequiredArgsConstructor
@RestController
@RequestMapping("/sync/task")
public class SyncTaskController extends BaseController {

    private final ISyncTaskService syncTaskService;
    private final ISeaTunnelJobService seaTunnelJobService;
    private final IDataConsistencyService dataConsistencyService;
    private final IDataSourceMetadataService metadataService;

    @SaCheckPermission("sync:task:list")
    @GetMapping("/list")
    public R<PageResult<SyncTaskVo>> list(SyncTaskBo bo, PageQuery pageQuery) {
        return R.ok(syncTaskService.queryPageList(bo, pageQuery));
    }

    @SaCheckPermission("sync:task:query")
    @GetMapping("/{taskId}")
    public R<SyncTaskVo> getInfo(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(syncTaskService.queryById(taskId));
    }

    @SaCheckPermission("sync:task:add")
    @Log(title = "同步任务", businessType = BusinessType.INSERT)
    @RepeatSubmit()
    @PostMapping
    public R<Void> add(@Validated @RequestBody SyncTaskBo bo) {
        return toAjax(syncTaskService.insertByBo(bo));
    }

    @SaCheckPermission("sync:task:edit")
    @Log(title = "同步任务", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PutMapping
    public R<Void> edit(@Validated @RequestBody SyncTaskBo bo) {
        return toAjax(syncTaskService.updateByBo(bo));
    }

    @SaCheckPermission("sync:task:remove")
    @Log(title = "同步任务", businessType = BusinessType.DELETE)
    @DeleteMapping("/{taskId}")
    public R<Void> remove(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return toAjax(syncTaskService.deleteById(taskId));
    }

    @SaCheckPermission("sync:task:validate")
    @Log(title = "同步任务校验", businessType = BusinessType.OTHER)
    @PostMapping("/{taskId}/validate")
    public R<SyncTaskValidationResult> validate(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(syncTaskService.validate(taskId));
    }

    @SaCheckPermission("sync:task:validate")
    @Log(title = "目标兼容性检查", businessType = BusinessType.OTHER)
    @PostMapping("/{taskId}/target-compatibility")
    public R<TargetCompatibilityVo> targetCompatibility(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(metadataService.checkTargetCompatibility(taskId));
    }

    @SaCheckPermission("sync:task:engine-config")
    @Log(title = "同步任务配置预览", businessType = BusinessType.OTHER)
    @PostMapping("/{taskId}/engine-config")
    public R<SeaTunnelJobConfigPreview> engineConfig(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(seaTunnelJobService.previewConfig(taskId));
    }

    @SaCheckPermission("sync:task:start")
    @Log(title = "启动同步任务", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PostMapping("/{taskId}/start")
    public R<SeaTunnelJobOperationResult> start(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(seaTunnelJobService.start(taskId));
    }

    @SaCheckPermission("sync:task:status")
    @Log(title = "刷新同步任务状态", businessType = BusinessType.OTHER)
    @PostMapping("/{taskId}/status")
    public R<SeaTunnelJobStatus> status(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(seaTunnelJobService.refreshStatus(taskId));
    }

    @SaCheckPermission("sync:task:check")
    @Log(title = "同步任务数据核对", businessType = BusinessType.OTHER)
    @PostMapping("/{taskId}/check")
    public R<SyncTaskDataCheckResult> check(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId,
                                            @RequestBody(required = false) SyncTaskDataCheckRequest request) {
        return R.ok(dataConsistencyService.check(taskId, request));
    }

    @SaCheckPermission("sync:task:pause")
    @Log(title = "暂停同步任务", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PostMapping("/{taskId}/pause")
    public R<SeaTunnelJobOperationResult> pause(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(seaTunnelJobService.pause(taskId));
    }

    @SaCheckPermission("sync:task:resume")
    @Log(title = "恢复同步任务", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PostMapping("/{taskId}/resume")
    public R<SeaTunnelJobOperationResult> resume(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(seaTunnelJobService.resume(taskId));
    }

    @SaCheckPermission("sync:task:stop")
    @Log(title = "停止同步任务", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PostMapping("/{taskId}/stop")
    public R<SeaTunnelJobOperationResult> stop(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(seaTunnelJobService.stop(taskId));
    }

    @SaCheckPermission("sync:task:reinitialize")
    @Log(title = "重新初始化同步任务", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PostMapping("/{taskId}/reinitialize")
    public R<SeaTunnelJobOperationResult> reinitialize(@NotNull(message = "任务ID不能为空") @PathVariable Long taskId) {
        return R.ok(seaTunnelJobService.reinitialize(taskId));
    }
}
