import type { PageQuery } from '@/api/types';

export interface SyncTaskForm {
  taskId?: string | number;
  taskName?: string;
  sourceId?: string | number;
  targetId?: string | number;
  sourceTable?: string;
  targetSchema?: string;
  targetTable?: string;
  syncMode?: string;
  incrementalStartupMode?: 'LATEST' | 'TIMESTAMP' | 'SPECIFIC' | string;
  incrementalStartupTimestamp?: string;
  incrementalStartupBinlogFile?: string;
  incrementalStartupBinlogPosition?: number;
  fullDataMode?: string;
  ddlPolicy?: string;
  scheduleMode?: 'ONCE' | 'CRON' | 'REALTIME' | 'MANUAL' | string;
  cronExpression?: string;
  configVersion?: number;
  selectedColumns?: string | string[];
  syncKeyColumns?: string;
  readLimitRowsPerSecond?: number;
  readLimitBytesPerSecond?: number;
  snapshotParallelism?: number;
  sourceConnectionLimit?: number;
}

export interface SyncTaskQuery extends PageQuery {
  taskName?: string;
  sourceId?: string | number;
  targetId?: string | number;
  status?: string;
}

export interface SyncTaskVO extends SyncTaskForm {
  taskId: string | number;
  status?: string;
  engineJobId?: string;
  engineConfigHash?: string;
  lastCheckpointId?: string;
  lastCheckpointTime?: string;
  lastCheckpointStatus?: string;
  kafkaPublishedCount?: number;
  kafkaLastPartition?: number;
  kafkaLastOffset?: number;
  kafkaLastSourceEventTime?: string;
  kafkaLastBrokerAckTime?: string;
  kafkaLagSeconds?: number;
  lastCheckSourceRows?: number;
  lastCheckTargetRows?: number;
  lastCheckDifference?: number;
  lastCheckMatched?: '0' | '1';
  lastCheckTime?: string;
  lastCheckMessage?: string;
  lastError?: string;
  createTime?: string;
  updateTime?: string;
  nextRunTime?: string;
  lastTriggerTime?: string;
  lastSkipReason?: string;
  overwriteStageTable?: string;
}

export interface TaskValidationResult {
  valid: boolean;
  message: string;
  source: { success: boolean; message: string; latencyMs: number };
  target: { success: boolean; message: string; latencyMs: number };
  cdcPrecheck?: {
    passed: boolean;
    message: string;
    serverId?: string;
    gtidMode?: string;
    binlogRetention?: string;
    checks?: Array<{ code: string; label: string; required: boolean; passed: boolean; actual?: string; message: string; suggestion?: string }>;
  };
  targetCompatibility?: TargetCompatibilityVO;
}

export interface TargetCompatibilityCheckItemVO {
  code: string;
  label: string;
  required: boolean;
  passed: boolean;
  actual?: string;
  message: string;
  suggestion?: string;
}

export interface TargetCompatibilityVO {
  taskId: string | number;
  sourceTable: string;
  targetTable: string;
  targetExists: boolean;
  passed: boolean;
  message: string;
  checks: TargetCompatibilityCheckItemVO[];
}

export interface SeaTunnelJobConfigPreview {
  taskId: string | number;
  jobName: string;
  sourceTable: string;
  targetTable: string;
  primaryKeys: string[];
  config: string;
}

export interface SeaTunnelJobOperationResult {
  taskId: string | number;
  engineJobId?: string;
  status: string;
  message: string;
}

export interface SeaTunnelJobStatus {
  taskId: string | number;
  engineJobId?: string;
  engineStatus?: string;
  status: string;
  errorMessage?: string;
  lastCheckpointId?: string;
  lastCheckpointTime?: string;
  lastCheckpointStatus?: string;
  phase?: 'SNAPSHOT' | 'CDC';
  sourceReceivedCount?: number;
  sinkCommittedCount?: number;
  sourceReceivedBytes?: number;
  sinkCommittedBytes?: number;
  sourceQps?: number;
  sinkQps?: number;
  cdcLagSeconds?: number;
  metricsMessage?: string;
}

export interface SyncTaskDataCheckResult {
  taskId: string | number;
  sourceTable: string;
  targetTable: string;
  sourceRows?: number;
  targetRows?: number;
  difference?: number;
  matched: boolean;
  success: boolean;
  message: string;
  checkMode?: 'COUNT' | 'KEY_RANGE';
  blockSize?: number;
  totalBlocks?: number;
  matchedBlocks?: number;
  mismatchedBlocks?: number;
  failedBlocks?: number;
  watermarkMessage?: string;
  blocks?: SyncTaskDataCheckBlock[];
}

export interface SyncTaskDataCheckBlock {
  index: number;
  lowerBound?: string;
  upperBound?: string;
  sourceRows?: number;
  targetRows?: number;
  difference?: number;
  matched: boolean;
  success: boolean;
  message?: string;
}

export interface SyncTaskDataCheckRequest {
  mode?: 'COUNT' | 'KEY_RANGE';
  blockSize?: number;
  strictWatermark?: boolean;
}

export interface SyncTaskMetrics {
  phase?: 'SNAPSHOT' | 'CDC';
  sourceReceivedCount?: number;
  sinkCommittedCount?: number;
  sourceReceivedBytes?: number;
  sinkCommittedBytes?: number;
  sourceQps?: number;
  sinkQps?: number;
  cdcLagSeconds?: number;
  metricsMessage?: string;
}
