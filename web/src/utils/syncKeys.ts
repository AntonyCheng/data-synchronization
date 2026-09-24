import type { DataSourceMetadataVO } from '@/api/sync/data-source/types';

/** Sync-key candidates shared by the single-table wizard and the task-group form. */

export interface SyncKeyOption {
  label: string;
  value: string;
}

function sameColumns(left: string[], right: string[]) {
  return left.length === right.length && left.every((column, index) => column === right[index]);
}

/**
 * Unique indexes that can carry a sync key (every column NOT NULL). The primary key's own index
 * is left out: a source may report it as a unique index too (MySQL's `PRIMARY`), and it is the
 * primary key, not a second key.
 */
export function reliableUniqueKeys(metadata?: DataSourceMetadataVO) {
  if (!metadata) return [];
  return metadata.uniqueKeys.filter(key => key.allNotNull && !sameColumns(key.columns, metadata.primaryKeys));
}

/**
 * Sync-key options: the primary key, then every reliable unique index. Each column list is
 * offered once (two indexes over the same columns are one key, and a Select needs unique values).
 */
export function syncKeyOptions(metadata?: DataSourceMetadataVO): SyncKeyOption[] {
  if (!metadata) return [];
  const candidates: SyncKeyOption[] = [
    ...(metadata.primaryKeys.length
      ? [{ label: `主键（${metadata.primaryKeys.join(', ')}）`, value: metadata.primaryKeys.join(',') }]
      : []),
    ...reliableUniqueKeys(metadata).map(key => ({
      label: `唯一键 ${key.name}（${key.columns.join(', ')}）`,
      value: key.columns.join(',')
    }))
  ];
  return candidates.filter(
    (option, index) => candidates.findIndex(candidate => candidate.value === option.value) === index
  );
}
