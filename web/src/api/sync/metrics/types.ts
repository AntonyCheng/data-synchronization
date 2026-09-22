/**
 * What a job is doing as far as the engine lets us know. MIXED is a FULL_CDC job past
 * startup: Zeta exposes no snapshot-complete signal, so snapshot and CDC cannot be told apart.
 */
export type SyncPhase = 'SNAPSHOT' | 'CDC' | 'MIXED';

export const SYNC_PHASE_LABELS: Record<SyncPhase, string> = {
  SNAPSHOT: '全量快照',
  CDC: 'CDC 增量',
  MIXED: '全量 + CDC（引擎不区分阶段）'
};

export function syncPhaseLabel(phase?: string): string {
  return (phase && SYNC_PHASE_LABELS[phase as SyncPhase]) || phase || '-';
}

/** One engine metrics sample (ds_sync_metrics_sample row) of a task or task-group item. */
export interface SyncMetricsSample {
  sampledAt: string;
  engineJobId?: string;
  engineStatus?: string;
  phase?: SyncPhase | string;
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
