/** One engine metrics sample (ds_sync_metrics_sample row) of a task or task-group item. */
export interface SyncMetricsSample {
  sampledAt: string;
  engineJobId?: string;
  engineStatus?: string;
  phase?: 'SNAPSHOT' | 'CDC' | string;
  sourceReceivedCount?: number;
  sinkCommittedCount?: number;
  sourceReceivedBytes?: number;
  sinkCommittedBytes?: number;
  sourceQps?: number;
  sinkQps?: number;
  /** Source received - sink committed at that instant (never negative). */
  backlogRows?: number;
  cdcLagSeconds?: number;
}

/** Trailing metrics window of one owner; samples oldest first. */
export interface SyncMetricsSeries {
  ownerType: 'TASK' | 'GROUP_ITEM';
  ownerId: string | number;
  windowMinutes: number;
  latest?: SyncMetricsSample;
  samples: SyncMetricsSample[];
}
