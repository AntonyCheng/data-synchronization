import type { PageQuery } from '@/api/types';

export interface DataSourceForm {
  sourceId?: string | number;
  sourceName?: string;
  sourceType?: 'MYSQL' | 'POSTGRESQL';
  host?: string;
  port?: number;
  databaseName?: string;
  schemaName?: string;
  username?: string;
  password?: string;
  sslEnabled?: string;
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

export interface DataSourceCdcPrecheckVO {
  sourceId: string | number;
  sourceType: string;
  serverId?: string;
  gtidMode?: string;
  binlogRetention?: string;
  passed: boolean;
  message: string;
  checks: DataSourceCheckItemVO[];
}

export interface ConnectionTestResult {
  success: boolean;
  message: string;
  latencyMs: number;
}

export interface DataSourceCredentialMigrationResult {
  enabled: boolean;
  total: number;
  migrated: number;
  message: string;
}
