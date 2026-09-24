package org.dromara.sync.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import org.dromara.common.log.annotation.Log;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.service.ISyncTaskGroupDataCheckService;
import org.dromara.sync.service.ISyncTaskGroupDdlService;
import org.dromara.sync.service.ISyncTaskGroupDiscoveryService;
import org.dromara.sync.service.ISyncTaskGroupService;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.PostMapping;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Discovery and the row-count check moved out of the group service into their own services; the
 * endpoints the console calls must not have moved with them.
 */
@Tag("dev")
class SyncTaskGroupControllerTest {

    private final ISyncTaskGroupService groupService = mock(ISyncTaskGroupService.class);
    private final ISyncTaskGroupDiscoveryService discoveryService = mock(ISyncTaskGroupDiscoveryService.class);
    private final ISyncTaskGroupDataCheckService dataCheckService = mock(ISyncTaskGroupDataCheckService.class);
    private final SyncTaskGroupController controller = new SyncTaskGroupController(groupService,
        mock(ISyncTaskGroupDdlService.class), discoveryService, dataCheckService, mock(ISyncMetricsService.class));

    @Test
    void discoverAndCheckAreServedByTheirOwnServices() {
        SyncTaskGroupOperationResult discovered = new SyncTaskGroupOperationResult();
        SyncTaskGroupDataCheckResult checked = new SyncTaskGroupDataCheckResult();
        when(discoveryService.discover(7L)).thenReturn(discovered);
        when(dataCheckService.checkData(7L)).thenReturn(checked);

        assertSame(discovered, controller.discover(7L).getData());
        assertSame(checked, controller.checkData(7L).getData());
        verifyNoInteractions(groupService);
    }

    @Test
    void theirRoutesPermissionsAndAuditTitlesAreUnchanged() throws NoSuchMethodException {
        assertEndpoint("discover", "/{groupId}/discover", "sync:group:validate", "扫描同步任务组新表");
        assertEndpoint("checkData", "/{groupId}/check", "sync:group:check", "同步任务组数据核对");
    }

    private static void assertEndpoint(String method, String path, String permission, String title) throws NoSuchMethodException {
        Method endpoint = SyncTaskGroupController.class.getMethod(method, Long.class);
        assertArrayEquals(new String[]{path}, endpoint.getAnnotation(PostMapping.class).value(), method);
        assertArrayEquals(new String[]{permission}, endpoint.getAnnotation(SaCheckPermission.class).value(), method);
        assertEquals(title, endpoint.getAnnotation(Log.class).title(), method);
    }
}
