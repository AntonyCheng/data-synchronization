import { CloseOutlined } from '@ant-design/icons';
import { Button, Tag, Tooltip, Typography } from 'antd';
import type { DataSourceMetadataVO, DataSourceVO } from '@/api/sync/data-source/types';
import type { SyncTaskForm } from '@/api/sync/task/types';
import { useUserStore } from '@/stores/userStore';
import { hasPermi } from '@/utils/permission';

/** Constants and presentational helpers shared by the task list, its detail modal and its wizard. */

export const defaultForm: SyncTaskForm = {
  syncMode: 'FULL_CDC',
  incrementalStartupMode: 'LATEST',
  fullDataMode: 'UPSERT',
  ddlPolicy: 'FAIL',
  scheduleMode: 'ONCE',
  targetSchema: 'public',
  kafkaOutputFormat: 'ENVELOPE',
  readLimitRowsPerSecond: 1000,
  readLimitBytesPerSecond: 10485760,
  snapshotParallelism: 1,
  sourceConnectionLimit: 2
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

export const statusLabels: Record<string, string> = {
  DRAFT: '草稿',
  RUNNING: '运行中',
  PAUSING: '暂停中',
  PAUSED: '已暂停',
  STOPPED: '已停止',
  FAILED: '失败',
  FINISHED: '已完成',
  REINITIALIZE_REQUIRED: '需重新初始化'
};

export function sourceLabel(source: DataSourceVO) {
  return `${source.sourceName} (${source.sourceType})`;
}

export function sourceTypeOf(dataSources: DataSourceVO[], sourceId?: string | number) {
  return dataSources.find(item => String(item.sourceId) === String(sourceId))?.sourceType;
}

export function statusTag(status?: string, error?: string) {
  const tag = (
    <Tag
      color={
        status === 'RUNNING'
          ? 'processing'
          : status === 'FAILED' || status === 'REINITIALIZE_REQUIRED'
            ? 'error'
            : status === 'PAUSED'
              ? 'warning'
              : 'default'
      }
    >
      {statusLabels[status || ''] || status || '-'}
    </Tag>
  );
  return error ? <Tooltip title={error}>{tag}</Tooltip> : tag;
}

/** Title row of a dismissable diagnostic Alert (CDC precheck, compatibility, validation, …). */
export function diagnosticMessage(title: string, onClose: () => void) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', gap: 12 }}>
      <Typography.Text strong>{title}</Typography.Text>
      <Button
        type="text"
        size="small"
        icon={<CloseOutlined />}
        aria-label="关闭检查结果"
        title="关闭检查结果"
        onClick={onClose}
      />
    </div>
  );
}

/** Sync-key candidates: the primary key, then unique indexes whose columns are all NOT NULL. */
export function reliableKeyOptions(metadata?: DataSourceMetadataVO) {
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

export function defaultKafkaTopic(database: string, table: string) {
  return `${database || 'source'}_${table}`.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 249);
}

export function mappingRisks(metadata?: DataSourceMetadataVO) {
  if (!metadata) return [];
  const risks: string[] = [];
  const ambiguous = metadata.columns
    .filter(column => /tinyint/i.test(column.typeName || '') && (column.size || 0) <= 1)
    .map(column => column.name);
  const large = metadata.columns
    .filter(column => /(text|blob|json)/i.test(column.typeName || ''))
    .map(column => column.name);
  if (ambiguous.length) risks.push(`字段 ${ambiguous.join(', ')} 可能是布尔语义 tinyint(1)，请确认目标端类型。`);
  if (large.length) risks.push(`字段 ${large.join(', ')} 为 TEXT/BLOB/JSON，可能显著增加读取字节量。`);
  if (metadata.charset && !/utf8|unicode/i.test(metadata.charset))
    risks.push(`源表字符集为 ${metadata.charset}，请关注旧编码的转换风险。`);
  return risks;
}

export function useTaskPermissions() {
  const userInfo = useUserStore(state => state.userInfo);
  return {
    canAdd: hasPermi(userInfo, ['sync:task:add']),
    canDetail: hasPermi(userInfo, ['sync:task:query']),
    canEdit: hasPermi(userInfo, ['sync:task:edit']),
    canRemove: hasPermi(userInfo, ['sync:task:remove']),
    canValidate: hasPermi(userInfo, ['sync:task:validate']),
    canCheck: hasPermi(userInfo, ['sync:task:check']),
    canPreview: hasPermi(userInfo, ['sync:task:engine-config']),
    canStart: hasPermi(userInfo, ['sync:task:start']),
    canStatus: hasPermi(userInfo, ['sync:task:status']),
    canPause: hasPermi(userInfo, ['sync:task:pause']),
    canResume: hasPermi(userInfo, ['sync:task:resume']),
    canStop: hasPermi(userInfo, ['sync:task:stop']),
    canReinitialize: hasPermi(userInfo, ['sync:task:reinitialize']),
    canCdcPrecheck: hasPermi(userInfo, ['sync:data-source:cdc-precheck'])
  };
}
