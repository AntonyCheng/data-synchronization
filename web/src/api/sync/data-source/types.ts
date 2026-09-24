import type { PageQuery } from '@/api/types';

export interface DataSourceForm {
  sourceId?: string | number;
  sourceName?: string;
  sourceType?: 'MYSQL' | 'POSTGRESQL' | 'KAFKA';
  host?: string;
  port?: number;
  databaseName?: string;
  schemaName?: string;
  username?: string;
  password?: string;
  sslEnabled?: string;
  /**
   * MySQL only: IANA id of the zone the source server renders TIMESTAMP in (e.g. `UTC`,
   * `Asia/Shanghai`). Blank = compatibility mode (Asia/Shanghai). Omitted on edit = keep.
   */
  serverTimeZone?: string;
  status?: string;
  remark?: string;
}

export interface DataSourceQuery extends PageQuery {
  sourceName?: string;
  sourceType?: string;
  status?: string;
}

export interface DataSourceVO extends Omit<DataSourceForm, 'password'> {
  sourceId: string | number;
  createTime?: string;
  updateTime?: string;
}

export interface DataSourceColumnVO {
  name: string;
  typeName?: string;
  jdbcType?: number;
  size?: number;
  scale?: number;
  nullable?: boolean;
  autoIncrement?: boolean;
}

export interface DataSourceIndexVO {
  name: string;
  unique?: boolean;
  columns: string[];
  allNotNull?: boolean;
}

export interface DataSourceMetadataVO {
  sourceId: string | number;
  sourceType: string;
  databaseName: string;
  tableName: string;
  charset?: string;
  collation?: string;
  columns: DataSourceColumnVO[];
  primaryKeys: string[];
  uniqueKeys: DataSourceIndexVO[];
}

export interface DataSourceCheckItemVO {
  code: string;
  label: string;
  required: boolean;
  passed: boolean;
  actual?: string;
  message: string;
  suggestion?: string;
}

/** A MySQL source server's time zone next to the one the engine is told for it (server-time-zone). */
export interface DataSourceTimeZoneVO {
  /** What the server reports, e.g. `SYSTEM（UTC）`, `+08:00`; absent when unreadable. */
  serverTimeZone?: string;
  /** e.g. `UTC+00:00`; absent when unreadable. */
  serverUtcOffset?: string;
  /** IANA id to configure for this server; absent when none can be inferred. */
  suggestedTimeZone?: string;
  /** The data source's configured zone; blank in compatibility mode. */
  configuredTimeZone?: string;
  /** Configured zone, else Asia/Shanghai. */
  effectiveTimeZone: string;
  /** Absent only when a stored zone is not a valid IANA id. */
  effectiveUtcOffset?: string;
  /** Offsets agree now; absent when the server's zone could not be read. */
  matched?: boolean;
  /** How far binlog-phase TIMESTAMP values drift, in hours (e.g. `8`, `-5.5`). */
  shiftHours?: string;
  message: string;
}

export interface DataSourceCdcPrecheckVO {
  sourceId: string | number;
  sourceType: string;
  serverId?: string;
  gtidMode?: string;
  binlogRetention?: string;
  timeZone?: DataSourceTimeZoneVO;
  passed: boolean;
  message: string;
  checks: DataSourceCheckItemVO[];
}

export interface ConnectionTestResult {
  success: boolean;
  message: string;
  latencyMs: number;
  /** MySQL only. */
  timeZone?: DataSourceTimeZoneVO;
}

export interface DataSourceCredentialMigrationResult {
  enabled: boolean;
  total: number;
  migrated: number;
  message: string;
}

export interface KafkaTopicVO {
  topic: string;
  partitions: number;
  replicationFactor: number;
}

/** Lightweight row for the wizards' source / target pickers (GET /sync/data-source/options). */
export interface DataSourceOptionVO {
  sourceId: number | string;
  sourceName: string;
  sourceType?: 'MYSQL' | 'POSTGRESQL' | 'KAFKA';
  databaseName?: string;
  /** '0' = enabled. */
  status?: string;
}
