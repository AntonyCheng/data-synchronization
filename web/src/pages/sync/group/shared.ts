import type { DataSourceMetadataVO, DataSourceVO } from '@/api/sync/data-source/types';
import type { SyncTaskGroupForm } from '@/api/sync/group/types';
import { useUserStore } from '@/stores/userStore';
import { hasPermi } from '@/utils/permission';

/** Constants and helpers shared by the group list, its detail console and its form. */

export const emptyForm: SyncTaskGroupForm = {
  syncScope: 'MULTI_TABLE',
  autoDiscover: '0',
  syncMode: 'FULL_CDC',
  ddlPolicy: 'FAIL',
  kafkaOutputFormat: 'ENVELOPE',
  readLimitRowsPerSecond: 1000,
  readLimitBytesPerSecond: 10485760,
  snapshotParallelism: 1,
  sourceConnectionLimit: 2,
  // No blank starter row: tables are added through the picker (one or many at a time), and an
  // empty item would just be an incomplete row that blocks validation on save.
  items: []
};

export const kafkaOutputFormatOptions = [
  { label: '默认JSON', value: 'ENVELOPE' },
  { label: 'Canal JSON', value: 'CANAL_JSON' },
  { label: '兼容 Debezium JSON', value: 'COMPATIBLE_DEBEZIUM_JSON' },
  { label: 'Maxwell JSON', value: 'MAXWELL_JSON' },
  { label: 'OGG JSON', value: 'OGG_JSON' }
];

export const kafkaOutputFormatLabel = (value?: string) =>
  kafkaOutputFormatOptions.find(option => option.value === value)?.label || value || '-';

export const groupStatusLabels: Record<string, string> = {
  DRAFT: '草稿',
  RUNNING: '运行中',
  PAUSING: '暂停中',
  PAUSED: '已暂停',
  DEGRADED: '部分异常',
  STOPPED: '已停止',
  FAILED: '失败',
  FINISHED: '已完成',
  VALID: '校验通过',
  INVALID: '校验失败'
};

export function groupStatusLabel(status?: string) {
  return status ? groupStatusLabels[status] || status : '草稿';
}

export const itemStatusLabels: Record<string, string> = {
  PENDING: '待启动',
  RUNNING: '运行中',
  PAUSING: '暂停中',
  PAUSED: '已暂停',
  STOPPED: '已停止',
  FAILED: '失败',
  DDL_BLOCKED: '结构变更阻塞',
  FINISHED: '已完成'
};

// Mirrors REINITIALIZABLE_ITEM_STATUSES on the backend.
export const reinitializableItemStatuses = ['FAILED', 'DDL_BLOCKED', 'STOPPED', 'FINISHED'];

export function itemStatusColor(status?: string) {
  if (status === 'RUNNING') return 'success';
  if (status === 'FAILED' || status === 'DDL_BLOCKED') return 'error';
  if (status === 'PAUSING' || status === 'PAUSED') return 'warning';
  return 'default';
}

/** Sync-key candidates: the primary key, then unique indexes whose columns are all NOT NULL. */
export function keyOptions(metadata?: DataSourceMetadataVO) {
  if (!metadata) return [];
  const options = metadata.primaryKeys.length
    ? [{ label: `主键（${metadata.primaryKeys.join(', ')}）`, value: metadata.primaryKeys.join(',') }]
    : [];
  metadata.uniqueKeys
    .filter(key => key.allNotNull)
    .forEach(key =>
      options.push({ label: `唯一键 ${key.name}（${key.columns.join(', ')}）`, value: key.columns.join(',') })
    );
  return options;
}

export function sourceTypeOf(dataSources: DataSourceVO[], sourceId?: string | number) {
  return dataSources.find(item => String(item.sourceId) === String(sourceId))?.sourceType;
}

/** Display name of one table item, schema-qualified only for PostgreSQL targets. */
export function itemLabel(
  item: { sourceDatabase?: string; sourceTable?: string; targetSchema?: string; targetTable?: string },
  targetType?: string
) {
  const target =
    targetType === 'POSTGRESQL' ? `${item.targetSchema || 'public'} . ${item.targetTable}` : item.targetTable;
  return `${item.sourceDatabase || '-'} . ${item.sourceTable} -> ${target}`;
}

export function useGroupPermissions() {
  const userInfo = useUserStore(state => state.userInfo);
  return (permission: string) => hasPermi(userInfo, [permission]);
}
