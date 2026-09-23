/** Server-side dashboard counters (GET /sync/overview). */
export interface SyncOverviewVO {
  /** One engine job per single-table task and per table item of a group. */
  jobTotal: number;
  jobRunning: number;
  jobFailed: number;
  taskTotal: number;
  taskRunning: number;
  groupTotal: number;
  groupRunning: number;
  groupItemTotal: number;
  groupItemRunning: number;
  dataSourceTotal: number;
  dataSourceEnabled: number;
  checkedRows: number;
  checkedJobs: number;
  checkedMatched: number;
  avgCdcLagSeconds?: number | null;
  statusCounts: Record<string, number>;
  targetTypeCounts: Record<string, number>;
  /** Same keys, restricted to jobs currently RUNNING. */
  targetTypeRunningCounts: Record<string, number>;
}
