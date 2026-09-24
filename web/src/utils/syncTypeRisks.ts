import type { DataSourceMetadataVO } from '@/api/sync/data-source/types';

/**
 * TIME columns with fractional seconds (`TIME(p)`, p > 0) among `selected` (all columns when it is
 * empty). Connector/J reports `DECIMAL_DIGITS` 0 for them, but `COLUMN_SIZE` grows past the eight
 * characters of `HH:MM:SS` (`TIME(3)` is 12).
 */
export function fractionalTimeColumns(metadata: DataSourceMetadataVO | undefined, selected?: string[]) {
  const pick = selected?.length ? new Set(selected.map(column => column.toLowerCase())) : undefined;
  return (metadata?.columns || [])
    .filter(column => /^time$/i.test(column.typeName || '') && (column.size || 0) > 8)
    .filter(column => !pick || pick.has(column.name.toLowerCase()))
    .map(column => column.name);
}

/**
 * Where SeaTunnel 2.3.13 truncates `TIME(p)` to whole seconds, as measured on the engine (see
 * docs/type-mapping-mysql-postgresql.md): the FULL-mode source reads TIME through `java.sql.Time`,
 * whatever the target, and the PostgreSQL sink writes it through `Time.valueOf`, whatever the mode.
 * MySQL targets in CDC modes keep the fraction; Kafka targets were not measured, so nothing is
 * claimed for them. No URL setting fixes either hop, so the operator is told before saving rather
 * than finding whole seconds in the target later. Returns the warning, or undefined.
 */
export function timePrecisionWarning(columns: string[], syncMode?: string, targetType?: string) {
  if (!columns.length) return undefined;
  const hops = [
    ...(syncMode === 'FULL' ? ['全量模式读取 TIME'] : []),
    ...(targetType === 'POSTGRESQL' ? ['PostgreSQL 目标写入 TIME'] : [])
  ];
  if (!hops.length) return undefined;
  return `字段 ${columns.join(', ')} 为带小数秒的 TIME：${hops.join('、')} 时会截断到整秒（SeaTunnel 2.3.13 的限制，无法通过配置规避），小数秒不会同步到目标端。`;
}

/** A selection stored either as a list or as the backend's comma-separated string. */
export function columnList(selected?: string[] | string) {
  if (Array.isArray(selected)) return selected;
  return String(selected || '')
    .split(',')
    .map(column => column.trim())
    .filter(Boolean);
}
