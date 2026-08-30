import type { PageResult, R } from '@/api/types';
import request from '@/api/request';
import type {
  SeaTunnelJobConfigPreview,
  SeaTunnelJobOperationResult,
  SeaTunnelJobStatus,
  SyncTaskDataCheckResult,
  SyncTaskDataCheckRequest,
  SyncTaskForm,
  SyncTaskQuery,
  SyncTaskVO,
  TargetCompatibilityVO,
  TaskValidationResult
} from './types';

export function listSyncTasks(query?: SyncTaskQuery) {
  return request<R<PageResult<SyncTaskVO>>>({ url: '/sync/task/list', method: 'get', params: query });
}

export function getSyncTask(id: string | number) {
  return request<R<SyncTaskVO>>({ url: `/sync/task/${id}`, method: 'get' });
}

export function addSyncTask(data: SyncTaskForm) {
  return request<R>({ url: '/sync/task', method: 'post', data });
}

export function updateSyncTask(data: SyncTaskForm) {
  return request<R>({ url: '/sync/task', method: 'put', data });
}

export function deleteSyncTask(id: string | number) {
  return request<R>({ url: `/sync/task/${id}`, method: 'delete' });
}

export function validateSyncTask(id: string | number) {
  return request<R<TaskValidationResult>>({ url: `/sync/task/${id}/validate`, method: 'post' });
}

export function checkTargetCompatibility(id: string | number) {
  return request<R<TargetCompatibilityVO>>({ url: `/sync/task/${id}/target-compatibility`, method: 'post' });
}

export function previewSyncTaskConfig(id: string | number) {
  return request<R<SeaTunnelJobConfigPreview>>({ url: `/sync/task/${id}/engine-config`, method: 'post' });
}

export function startSyncTask(id: string | number) {
  return request<R<SeaTunnelJobOperationResult>>({ url: `/sync/task/${id}/start`, method: 'post' });
}

export function refreshSyncTaskStatus(id: string | number) {
  return request<R<SeaTunnelJobStatus>>({ url: `/sync/task/${id}/status`, method: 'post' });
}

export function checkSyncTaskData(id: string | number, data?: SyncTaskDataCheckRequest) {
  return request<R<SyncTaskDataCheckResult>>({ url: `/sync/task/${id}/check`, method: 'post', data });
}

export function pauseSyncTask(id: string | number) {
  return request<R<SeaTunnelJobOperationResult>>({ url: `/sync/task/${id}/pause`, method: 'post' });
}

export function resumeSyncTask(id: string | number) {
  return request<R<SeaTunnelJobOperationResult>>({ url: `/sync/task/${id}/resume`, method: 'post' });
}

export function stopSyncTask(id: string | number) {
  return request<R<SeaTunnelJobOperationResult>>({ url: `/sync/task/${id}/stop`, method: 'post' });
}

export function reinitializeSyncTask(id: string | number) {
  return request<R<SeaTunnelJobOperationResult>>({ url: `/sync/task/${id}/reinitialize`, method: 'post' });
}
