package org.dromara.sync.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.domain.PageResult;
import org.dromara.common.core.domain.R;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.common.web.core.BaseController;
import org.dromara.sync.domain.bo.SyncTaskGroupBo;
import org.dromara.sync.domain.vo.SyncTaskGroupConfigPreview;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupValidationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupStatus;
import org.dromara.sync.service.ISyncTaskGroupService;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RequiredArgsConstructor
@RestController
@RequestMapping("/sync/group")
public class SyncTaskGroupController extends BaseController {

    private final ISyncTaskGroupService groupService;

    @SaCheckPermission("sync:group:list")
    @GetMapping("/list")
    public R<PageResult<SyncTaskGroupVo>> list(String groupName, String status, PageQuery pageQuery) {
        return R.ok(groupService.queryPageList(groupName, status, pageQuery));
    }

    @SaCheckPermission("sync:group:query")
    @GetMapping("/{groupId}")
    public R<SyncTaskGroupVo> get(@PathVariable Long groupId) {
        return R.ok(groupService.queryById(groupId));
    }

    @SaCheckPermission("sync:group:add")
    @PostMapping
    public R<Void> add(@Validated @RequestBody SyncTaskGroupBo bo) {
        return toAjax(groupService.insertByBo(bo));
    }

    @SaCheckPermission("sync:group:edit")
    @PutMapping
    public R<Void> edit(@Validated @RequestBody SyncTaskGroupBo bo) {
        return toAjax(groupService.updateByBo(bo));
    }

    @SaCheckPermission("sync:group:remove")
    @DeleteMapping("/{groupId}")
    public R<Void> remove(@PathVariable Long groupId) {
        return toAjax(groupService.deleteById(groupId));
    }

    @SaCheckPermission("sync:group:validate")
    @PostMapping("/{groupId}/validate")
    public R<SyncTaskGroupValidationResult> validate(@PathVariable Long groupId) {
        return R.ok(groupService.validate(groupId));
    }

    @SaCheckPermission("sync:group:engine-config")
    @PostMapping("/{groupId}/engine-config")
    public R<SyncTaskGroupConfigPreview> engineConfig(@PathVariable Long groupId) {
        return R.ok(groupService.previewConfig(groupId));
    }

    @SaCheckPermission("sync:group:start")
    @PostMapping("/{groupId}/start")
    public R<SyncTaskGroupOperationResult> start(@PathVariable Long groupId) { return R.ok(groupService.start(groupId)); }

    @SaCheckPermission("sync:group:validate")
    @PostMapping("/{groupId}/discover")
    public R<SyncTaskGroupOperationResult> discover(@PathVariable Long groupId) { return R.ok(groupService.discover(groupId)); }

    @SaCheckPermission("sync:group:ddl-check")
    @PostMapping("/{groupId}/ddl-check")
    public R<SyncTaskGroupDdlCheckResult> checkDdl(@PathVariable Long groupId) { return R.ok(groupService.checkDdl(groupId)); }

    @SaCheckPermission("sync:group:check")
    @PostMapping("/{groupId}/check")
    public R<SyncTaskGroupDataCheckResult> checkData(@PathVariable Long groupId) { return R.ok(groupService.checkData(groupId)); }

    @SaCheckPermission("sync:group:resume")
    @PostMapping("/{groupId}/item/{itemId}/resume-after-ddl")
    public R<SyncTaskGroupOperationResult> resumeDdlItem(@PathVariable Long groupId, @PathVariable Long itemId) {
        return R.ok(groupService.resumeDdlItem(groupId, itemId));
    }

    @SaCheckPermission("sync:group:status")
    @PostMapping("/{groupId}/status")
    public R<SyncTaskGroupStatus> status(@PathVariable Long groupId) { return R.ok(groupService.refreshStatus(groupId)); }

    @SaCheckPermission("sync:group:pause")
    @PostMapping("/{groupId}/pause")
    public R<SyncTaskGroupOperationResult> pause(@PathVariable Long groupId) { return R.ok(groupService.pause(groupId)); }

    @SaCheckPermission("sync:group:resume")
    @PostMapping("/{groupId}/resume")
    public R<SyncTaskGroupOperationResult> resume(@PathVariable Long groupId) { return R.ok(groupService.resume(groupId)); }

    @SaCheckPermission("sync:group:stop")
    @PostMapping("/{groupId}/stop")
    public R<SyncTaskGroupOperationResult> stop(@PathVariable Long groupId) { return R.ok(groupService.stop(groupId)); }
}
