import type { PageResult, R } from '@/api/types';
import request from '@/api/request';
import type { SyncTaskGroupConfigPreview, SyncTaskGroupDataCheckResult, SyncTaskGroupDdlCheckResult, SyncTaskGroupForm, SyncTaskGroupQuery, SyncTaskGroupValidationResult, SyncTaskGroupVO } from './types';

export function listSyncTaskGroups(query?: SyncTaskGroupQuery) {
  return request<R<PageResult<SyncTaskGroupVO>>>({ url: '/sync/group/list', method: 'get', params: query });
}

export function getSyncTaskGroup(id: string | number) {
  return request<R<SyncTaskGroupVO>>({ url: `/sync/group/${id}`, method: 'get' });
}

export function addSyncTaskGroup(data: SyncTaskGroupForm) {
  return request<R>({ url: '/sync/group', method: 'post', data });
}

export function updateSyncTaskGroup(data: SyncTaskGroupForm) {
  return request<R>({ url: '/sync/group', method: 'put', data });
}

export function deleteSyncTaskGroup(id: string | number) {
  return request<R>({ url: `/sync/group/${id}`, method: 'delete' });
}

export function validateSyncTaskGroup(id: string | number) {
  return request<R<SyncTaskGroupValidationResult>>({ url: `/sync/group/${id}/validate`, method: 'post' });
}

export function previewSyncTaskGroupConfig(id: string | number) {
  return request<R<SyncTaskGroupConfigPreview>>({ url: `/sync/group/${id}/engine-config`, method: 'post' });
}

export function startSyncTaskGroup(id: string | number) { return request<R<{ groupId: string | number; status: string; message: string }>>({ url: `/sync/group/${id}/start`, method: 'post' }); }
export function discoverSyncTaskGroupTables(id: string | number) { return request<R<{ groupId: string | number; status: string; message: string }>>({ url: `/sync/group/${id}/discover`, method: 'post' }); }
export function checkSyncTaskGroupDdl(id: string | number) { return request<R<SyncTaskGroupDdlCheckResult>>({ url: `/sync/group/${id}/ddl-check`, method: 'post' }); }
export function checkSyncTaskGroupData(id: string | number) { return request<R<SyncTaskGroupDataCheckResult>>({ url: `/sync/group/${id}/check`, method: 'post' }); }
export function resumeSyncTaskGroupItemAfterDdl(groupId: string | number, itemId: string | number) { return request<R<{ groupId: string | number; status: string; message: string }>>({ url: `/sync/group/${groupId}/item/${itemId}/resume-after-ddl`, method: 'post' }); }
export function refreshSyncTaskGroupStatus(id: string | number) { return request<R<{ groupId: string | number; status: string; message: string }>>({ url: `/sync/group/${id}/status`, method: 'post' }); }
export function pauseSyncTaskGroup(id: string | number) { return request<R<{ groupId: string | number; status: string; message: string }>>({ url: `/sync/group/${id}/pause`, method: 'post' }); }
export function resumeSyncTaskGroup(id: string | number) { return request<R<{ groupId: string | number; status: string; message: string }>>({ url: `/sync/group/${id}/resume`, method: 'post' }); }
export function stopSyncTaskGroup(id: string | number) { return request<R<{ groupId: string | number; status: string; message: string }>>({ url: `/sync/group/${id}/stop`, method: 'post' }); }
