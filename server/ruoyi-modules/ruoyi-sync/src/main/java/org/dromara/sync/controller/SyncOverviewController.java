package org.dromara.sync.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.domain.R;
import org.dromara.common.web.core.BaseController;
import org.dromara.sync.domain.vo.SyncOverviewVo;
import org.dromara.sync.service.ISyncOverviewService;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Dashboard counters. Read-only; every number is computed over all rows, not a first page. */
@Validated
@RequiredArgsConstructor
@RestController
@RequestMapping("/sync/overview")
public class SyncOverviewController extends BaseController {

    private final ISyncOverviewService overviewService;

    @SaCheckPermission("sync:task:list")
    @GetMapping
    public R<SyncOverviewVo> overview() {
        return R.ok(overviewService.overview());
    }
}
