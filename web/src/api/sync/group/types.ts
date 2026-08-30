import type { PageQuery } from '@/api/types';

export interface SyncTaskGroupItemForm {
  itemId?: string | number;
  sourceDatabase?: string;
  sourceTable?: string;
  targetSchema?: string;
  targetTable?: string;
  primaryKeys?: string;
  ddlPolicy?: string;
  selectedColumns?: string | string[];
  syncKeyColumns?: string;
}

export interface SyncTaskGroupForm {
  groupId?: string | number;
  groupName?: string;
  sourceId?: string | number;
  targetId?: string | number;
  syncScope?: 'MULTI_TABLE' | 'DATABASE';
  sourceDatabase?: string;
  autoDiscover?: '0' | '1';
  syncMode?: string;
  ddlPolicy?: string;
  readLimitRowsPerSecond?: number;
  readLimitBytesPerSecond?: number;
  snapshotParallelism?: number;
  sourceConnectionLimit?: number;
  items?: SyncTaskGroupItemForm[];
}

export interface SyncTaskGroupQuery extends PageQuery {
  groupName?: string;
  status?: string;
}

export interface SyncTaskGroupItemVO extends SyncTaskGroupItemForm {
  itemId: string | number;
  groupId: string | number;
  status?: string;
  engineJobId?: string;
  lastError?: string;
  schemaHash?: string;
  lastCheckSourceRows?: number;
  lastCheckTargetRows?: number;
  lastCheckDifference?: number;
  lastCheckMatched?: '0' | '1';
  lastCheckTime?: string;
  lastCheckMessage?: string;
}

export interface SyncTaskGroupVO extends Omit<SyncTaskGroupForm, 'items'> {
  groupId: string | number;
  status?: string;
  configVersion?: number;
  engineJobId?: string;
  lastCheckpointId?: string;
  lastCheckpointTime?: string;
  lastCheckpointStatus?: string;
  lastError?: string;
  items: SyncTaskGroupItemVO[];
}

export interface SyncTaskGroupItemValidationResult {
  itemId: string | number;
  sourceTable: string;
  targetTable: string;
  passed: boolean;
  message: string;
}

export interface SyncTaskGroupValidationResult {
  groupId: string | number;
  valid: boolean;
  message: string;
  source: { success: boolean; message: string; latencyMs: number };
  target: { success: boolean; message: string; latencyMs: number };
  cdcPrecheck?: { passed: boolean; message: string };
  items: SyncTaskGroupItemValidationResult[];
}

export interface SyncTaskGroupConfigPreview {
  groupId: string | number;
  groupName: string;
  configVersion?: number;
  engineJobName: string;
  tableCount: number;
  config: string;
}

export interface SyncTaskGroupDdlEventVO {
  eventId: string | number;
  itemId: string | number;
  sourceTable: string;
  targetTable: string;
  changeType: string;
  riskLevel: 'LOW' | 'HIGH';
  status: 'PENDING_FIX' | 'READY_TO_RESUME' | 'RESOLVED';
  details: string;
  remediation: string;
  detectedAt?: string;
}

export interface SyncTaskGroupDdlCheckResult {
  groupId: string | number;
  status: string;
  message: string;
  events: SyncTaskGroupDdlEventVO[];
}

export interface SyncTaskGroupDataCheckItemResult {
  itemId: string | number;
  sourceTable: string;
  targetTable: string;
  sourceRows?: number;
  targetRows?: number;
  difference?: number;
  matched: boolean;
  success: boolean;
  message: string;
}

export interface SyncTaskGroupDataCheckResult {
  groupId: string | number;
  tableCount: number;
  matchedTableCount: number;
  mismatchedTableCount: number;
  failedTableCount: number;
  matched: boolean;
  success: boolean;
  message: string;
  consistencyNote: string;
  items: SyncTaskGroupDataCheckItemResult[];
}
