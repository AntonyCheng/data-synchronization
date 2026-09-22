import {
  CheckCircleOutlined,
  CodeOutlined,
  DeleteOutlined,
  EditOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
  StopOutlined
} from '@ant-design/icons';
import {
  Alert,
  Button,
  Descriptions,
  Divider,
  InputNumber,
  message,
  Modal,
  Select,
  Space,
  Table,
  Tag,
  Typography
} from 'antd';
import { useEffect, useRef, useState } from 'react';
import type { DataSourceCdcPrecheckVO, DataSourceVO } from '@/api/sync/data-source/types';
import type { SeaTunnelJobStatus, SyncTaskVO, TaskValidationResult } from '@/api/sync/task/types';
import { checkDataSourceCdc } from '@/api/sync/data-source';
import { syncPhaseLabel } from '@/api/sync/metrics/types';
import {
  checkSyncTaskData,
  checkTargetCompatibility,
  deleteSyncTask,
  getSyncTask,
  getSyncTaskMetrics,
  pauseSyncTask,
  previewSyncTaskConfig,
  refreshSyncTaskStatus,
  reinitializeSyncTask,
  resumeSyncTask,
  startSyncTask,
  stopSyncTask,
  validateSyncTask
} from '@/api/sync/task';
import MetricsHistoryPanel from '@/components/sync/MetricsHistoryPanel';
import {
  diagnosticMessage,
  kafkaOutputFormatLabel,
  sourceTypeOf,
  statusTag,
  useTaskPermissions
} from '@/pages/sync/task/shared';

export interface TaskDetailModalProps {
  open: boolean;
  /** The clicked row; the modal reloads it from the server and keeps the fresh copy. */
  task?: SyncTaskVO;
  dataSources: DataSourceVO[];
  onClose: () => void;
  /** The task changed server-side (action, delete) - the list should reload. */
  onChanged: () => void;
  onEdit: (task: SyncTaskVO) => void;
}

/**
 * Everything an operator does to one task: run control, diagnostics (CDC precheck,
 * target compatibility, validation, data check, engine config) and run metrics. The
 * diagnostics results live here, so opening the wizard never inherits them.
 */
export default function TaskDetailModal({ open, task, dataSources, onClose, onChanged, onEdit }: TaskDetailModalProps) {
  const permissions = useTaskPermissions();
  const pollingTokenRef = useRef(0);
  const [detail, setDetail] = useState<SyncTaskVO | undefined>(task);
  const [detailLoading, setDetailLoading] = useState(false);
  const [busyAction, setBusyAction] = useState<string>();
  const [previewVisible, setPreviewVisible] = useState(false);
  const [previewText, setPreviewText] = useState('');
  const [previewTitle, setPreviewTitle] = useState('SeaTunnel 配置预览');
  const [checkVisible, setCheckVisible] = useState(false);
  const [cdcVisible, setCdcVisible] = useState(false);
  const [compatibilityVisible, setCompatibilityVisible] = useState(false);
  const [validationVisible, setValidationVisible] = useState(false);
  const [cdcPrecheck, setCdcPrecheck] = useState<DataSourceCdcPrecheckVO | TaskValidationResult['cdcPrecheck']>();
  const [checkResult, setCheckResult] = useState<Awaited<ReturnType<typeof checkSyncTaskData>>['data']>();
  const [checkMode, setCheckMode] = useState<'COUNT' | 'KEY_RANGE'>('KEY_RANGE');
  const [checkBlockSize, setCheckBlockSize] = useState(10000);
  const [compatibilityResult, setCompatibilityResult] =
    useState<Awaited<ReturnType<typeof checkTargetCompatibility>>['data']>();
  const [validationResult, setValidationResult] = useState<Awaited<ReturnType<typeof validateSyncTask>>['data']>();
  const [metricsResult, setMetricsResult] = useState<SeaTunnelJobStatus>();

  const taskId = task?.taskId;

  useEffect(() => {
    if (!open || !taskId) return;
    // A newly opened task starts from the clicked row, then gets the authoritative copy.
    pollingTokenRef.current += 1;
    setDetail(task);
    setDetailLoading(true);
    getSyncTask(taskId)
      .then(result => setDetail(result.data))
      .finally(() => setDetailLoading(false));
    return () => {
      pollingTokenRef.current += 1;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, taskId]);

  const loadDetail = async (id: string | number, showLoading = true) => {
    if (showLoading) setDetailLoading(true);
    try {
      const result = await getSyncTask(id);
      setDetail(result.data);
      return result.data;
    } finally {
      if (showLoading) setDetailLoading(false);
    }
  };

  const refreshListAndDetail = async (id: string | number) => {
    await loadDetail(id, false);
    onChanged();
  };

  /** Pause / start land in a transient engine state; poll until it settles or the modal closes. */
  const pollStatus = async (id: string | number, pendingStatuses: string[]) => {
    const token = pollingTokenRef.current;
    for (let attempt = 0; attempt < 15; attempt += 1) {
      await new Promise(resolve => window.setTimeout(resolve, 2000));
      if (token !== pollingTokenRef.current) return;
      try {
        const result = await refreshSyncTaskStatus(id);
        await refreshListAndDetail(id);
        if (!result.data?.status || !pendingStatuses.includes(result.data.status)) return;
      } catch {
        return;
      }
    }
  };

  const runAction = async (
    key: string,
    action: (id: string | number) => Promise<{ data?: { message?: string } }>,
    pendingStatuses: string[] = []
  ) => {
    if (!detail || busyAction) return;
    const id = detail.taskId;
    setBusyAction(key);
    try {
      const result = await action(id);
      message.success(result.data?.message || '操作成功');
      await refreshListAndDetail(id);
      if (pendingStatuses.length > 0) await pollStatus(id, pendingStatuses);
    } catch {
      await loadDetail(id, false);
    } finally {
      setBusyAction(undefined);
    }
  };

  const remove = async () => {
    if (!detail) return;
    setBusyAction('delete');
    try {
      await deleteSyncTask(detail.taskId);
      message.success('删除成功');
      close();
      onChanged();
    } finally {
      setBusyAction(undefined);
    }
  };

  const validate = async () => {
    if (!detail) return;
    setBusyAction('validate');
    setValidationResult(undefined);
    try {
      const result = await validateSyncTask(detail.taskId);
      setValidationResult(result.data);
      setValidationVisible(true);
      setCdcPrecheck(result.data.cdcPrecheck);
      setCdcVisible(Boolean(result.data.cdcPrecheck));
      setCompatibilityResult(result.data.targetCompatibility);
      setCompatibilityVisible(Boolean(result.data.targetCompatibility));
      message[result.data.valid ? 'success' : 'error'](result.data.message);
      await refreshListAndDetail(detail.taskId);
    } finally {
      setBusyAction(undefined);
    }
  };

  const runCdcPrecheck = async () => {
    if (!detail?.sourceId) return;
    setBusyAction('cdc-precheck');
    try {
      const result = await checkDataSourceCdc(detail.sourceId);
      setCdcPrecheck(result.data);
      setCdcVisible(true);
      message[result.data?.passed ? 'success' : 'error'](result.data?.message || 'CDC 前置检查完成');
    } finally {
      setBusyAction(undefined);
    }
  };

  const runTargetCompatibility = async () => {
    if (!detail) return;
    setBusyAction('target-compatibility');
    try {
      const result = await checkTargetCompatibility(detail.taskId);
      setCompatibilityResult(result.data);
      setCompatibilityVisible(true);
      message[result.data?.passed ? 'success' : 'error'](result.data?.message || '目标兼容性检查完成');
    } finally {
      setBusyAction(undefined);
    }
  };

  const checkData = async () => {
    if (!detail) return;
    setBusyAction('check');
    try {
      const result = await checkSyncTaskData(detail.taskId, {
        mode: checkMode,
        blockSize: checkBlockSize,
        strictWatermark: true
      });
      setCheckResult(result.data);
      setCheckVisible(true);
    } finally {
      setBusyAction(undefined);
    }
  };

  const previewConfig = async () => {
    if (!detail) return;
    setBusyAction('engine-config');
    try {
      const result = await previewSyncTaskConfig(detail.taskId);
      setPreviewTitle(`${result.data?.jobName || detail.taskName} 配置预览`);
      setPreviewText(result.data?.config || '');
      setPreviewVisible(true);
    } finally {
      setBusyAction(undefined);
    }
  };

  const refreshStatus = async () => {
    if (!detail) return;
    setBusyAction('status');
    try {
      const result = await refreshSyncTaskStatus(detail.taskId);
      setMetricsResult(result.data);
      message.success(`引擎状态：${result.data?.engineStatus || result.data?.status || '-'}`);
      await refreshListAndDetail(detail.taskId);
    } finally {
      setBusyAction(undefined);
    }
  };

  const close = () => {
    pollingTokenRef.current += 1;
    setBusyAction(undefined);
    setPreviewVisible(false);
    setCheckVisible(false);
    setCheckMode('KEY_RANGE');
    setCheckBlockSize(10000);
    setCdcVisible(false);
    setCompatibilityVisible(false);
    setValidationVisible(false);
    setCdcPrecheck(undefined);
    setCompatibilityResult(undefined);
    setValidationResult(undefined);
    setMetricsResult(undefined);
    onClose();
  };

  const targetType = sourceTypeOf(dataSources, detail?.targetId);
  const targetIsKafka = targetType === 'KAFKA';
  const targetIsPostgres = targetType === 'POSTGRESQL';
  const taskStatus = detail?.status || '';
  const actionDisabled = Boolean(busyAction);
  const configLocked = ['RUNNING', 'PAUSING'].includes(taskStatus);

  return (
    <Modal
      title={detail ? `任务详情：${detail.taskName}` : '任务详情'}
      open={open}
      width={960}
      footer={null}
      destroyOnHidden
      onCancel={close}
    >
      {detail && (
        <Space direction="vertical" size={18} style={{ width: '100%' }}>
          <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }}>
            <Descriptions.Item label="任务状态">{statusTag(detail.status, detail.lastError)}</Descriptions.Item>
            <Descriptions.Item label="同步模式">
              {detail.syncMode === 'FULL_CDC' ? '全量 + CDC' : detail.syncMode || '-'}
            </Descriptions.Item>
            <Descriptions.Item label="调度模式">
              {detail.scheduleMode === 'CRON'
                ? `定时 Cron${detail.cronExpression ? ` (${detail.cronExpression})` : ''}`
                : detail.scheduleMode === 'REALTIME'
                  ? '常驻实时'
                  : detail.scheduleMode === 'ONCE'
                    ? '一次性'
                    : '手动'}
            </Descriptions.Item>
            <Descriptions.Item label="下次执行">{detail.nextRunTime || '-'}</Descriptions.Item>
            <Descriptions.Item label="SeaTunnel Job ID">{detail.engineJobId || '-'}</Descriptions.Item>
            <Descriptions.Item label="源表">{detail.sourceTable || '-'}</Descriptions.Item>
            <Descriptions.Item label="目标表">
              {targetIsPostgres
                ? `${detail.targetSchema || 'public'}.${detail.targetTable || '-'}`
                : detail.targetTable || '-'}
            </Descriptions.Item>
            <Descriptions.Item label="最近检查点">{detail.lastCheckpointTime || '-'}</Descriptions.Item>
            <Descriptions.Item label="最近核对">
              {detail.lastCheckTime
                ? `${detail.lastCheckTime} / ${detail.lastCheckMatched === '1' ? '一致' : '不一致'}`
                : '未核对'}
            </Descriptions.Item>
            {targetIsKafka && (
              <Descriptions.Item label="Kafka 输出格式">
                {kafkaOutputFormatLabel(detail.kafkaOutputFormat)}
              </Descriptions.Item>
            )}
            {targetIsKafka && (
              <Descriptions.Item label="Kafka 已发布事件">{detail.kafkaPublishedCount ?? '-'}</Descriptions.Item>
            )}
            {targetIsKafka && (
              <Descriptions.Item label="Kafka 最近分区/offset">
                {detail.kafkaLastPartition == null
                  ? '-'
                  : `${detail.kafkaLastPartition} / ${detail.kafkaLastOffset ?? '-'}`}
              </Descriptions.Item>
            )}
            {targetIsKafka && (
              <Descriptions.Item label="Kafka 最近源事件时间">
                {detail.kafkaLastSourceEventTime || '-'}
              </Descriptions.Item>
            )}
            {targetIsKafka && (
              <Descriptions.Item label="Kafka 最近 broker 确认">
                {detail.kafkaLastBrokerAckTime || '-'}
              </Descriptions.Item>
            )}
            {targetIsKafka && (
              <Descriptions.Item label="Kafka 发布延迟">
                {detail.kafkaLagSeconds == null ? '-' : `${detail.kafkaLagSeconds} 秒`}
              </Descriptions.Item>
            )}
            <Descriptions.Item label="最大行数/秒">{detail.readLimitRowsPerSecond ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="最大字节/秒">{detail.readLimitBytesPerSecond ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="快照并行度">{detail.snapshotParallelism ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="CDC 连接池上限">{detail.sourceConnectionLimit ?? '-'}</Descriptions.Item>
          </Descriptions>
          {metricsResult && (
            <Alert
              type="info"
              showIcon
              message="实时运行指标"
              description={
                <Descriptions size="small" column={{ xs: 1, sm: 2, md: 3 }}>
                  <Descriptions.Item label="当前阶段">{syncPhaseLabel(metricsResult.phase)}</Descriptions.Item>
                  <Descriptions.Item label="源端已读取">
                    {metricsResult.sourceReceivedCount ?? '-'} 行 / {metricsResult.sourceReceivedBytes ?? '-'} 字节
                  </Descriptions.Item>
                  <Descriptions.Item label="目标已提交">
                    {metricsResult.sinkCommittedCount ?? '-'} 行 / {metricsResult.sinkCommittedBytes ?? '-'} 字节
                  </Descriptions.Item>
                  <Descriptions.Item label="源端吞吐">
                    {metricsResult.sourceQps == null ? '-' : `${metricsResult.sourceQps.toFixed(2)} 行/秒`}
                  </Descriptions.Item>
                  <Descriptions.Item label="目标吞吐">
                    {metricsResult.sinkQps == null ? '-' : `${metricsResult.sinkQps.toFixed(2)} 行/秒`}
                  </Descriptions.Item>
                  <Descriptions.Item label="CDC 延迟">
                    {metricsResult.cdcLagSeconds == null
                      ? metricsResult.metricsMessage || '暂不可计算'
                      : `${metricsResult.cdcLagSeconds} 秒`}
                  </Descriptions.Item>
                </Descriptions>
              }
            />
          )}
          <Divider titlePlacement="left" plain>
            运行指标历史
          </Divider>
          <MetricsHistoryPanel
            load={minutes => getSyncTaskMetrics(detail.taskId, minutes).then(result => result.data)}
          />
          {detail.lastError && (
            <Alert
              type="error"
              showIcon
              message="最近一次错误"
              description={<Typography.Text copyable>{detail.lastError}</Typography.Text>}
            />
          )}
          {detail.lastSkipReason && (
            <Alert type="warning" showIcon message="最近一次调度未执行" description={detail.lastSkipReason} />
          )}
          <Divider titlePlacement="left" plain>
            准备与诊断
          </Divider>
          <Space wrap>
            {permissions.canCdcPrecheck && detail.syncMode === 'FULL_CDC' && (
              <Button
                icon={<SafetyCertificateOutlined />}
                loading={busyAction === 'cdc-precheck'}
                disabled={actionDisabled}
                onClick={runCdcPrecheck}
              >
                CDC 前置检查
              </Button>
            )}
            {permissions.canValidate && (
              <Button
                icon={<SafetyCertificateOutlined />}
                loading={busyAction === 'target-compatibility'}
                disabled={actionDisabled}
                onClick={runTargetCompatibility}
              >
                目标兼容性
              </Button>
            )}
            {permissions.canValidate && (
              <Button
                icon={<SafetyCertificateOutlined />}
                loading={busyAction === 'validate'}
                disabled={actionDisabled}
                onClick={validate}
              >
                校验任务
              </Button>
            )}
            {permissions.canCheck && (
              <Space.Compact>
                <Select
                  aria-label="数据核对模式"
                  value={checkMode}
                  onChange={value => setCheckMode(value)}
                  options={[
                    { label: '同步键分块', value: 'KEY_RANGE' },
                    { label: '整表行数', value: 'COUNT' }
                  ]}
                  disabled={actionDisabled}
                  style={{ width: 120 }}
                />
                {checkMode === 'KEY_RANGE' && (
                  <InputNumber
                    aria-label="核对分块步长"
                    min={1}
                    max={1000000}
                    value={checkBlockSize}
                    onChange={value => setCheckBlockSize(value || 10000)}
                    disabled={actionDisabled}
                    style={{ width: 110 }}
                  />
                )}
                <Button
                  icon={<CheckCircleOutlined />}
                  loading={busyAction === 'check'}
                  disabled={actionDisabled}
                  onClick={checkData}
                >
                  数据核对
                </Button>
              </Space.Compact>
            )}
            {permissions.canPreview && (
              <Button
                icon={<CodeOutlined />}
                loading={busyAction === 'engine-config'}
                disabled={actionDisabled}
                onClick={previewConfig}
              >
                查看引擎配置
              </Button>
            )}
            {permissions.canStatus && detail.engineJobId && (
              <Button
                icon={<ReloadOutlined />}
                loading={busyAction === 'status'}
                disabled={actionDisabled}
                onClick={refreshStatus}
              >
                刷新状态
              </Button>
            )}
          </Space>
          {cdcPrecheck && cdcVisible && (
            <Alert
              style={{ width: '100%' }}
              type={cdcPrecheck.passed ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage(cdcPrecheck.message, () => setCdcVisible(false))}
              description={
                <Descriptions size="small" column={{ xs: 1, sm: 2 }}>
                  <Descriptions.Item label="server-id">{cdcPrecheck.serverId || '-'}</Descriptions.Item>
                  <Descriptions.Item label="GTID 模式">{cdcPrecheck.gtidMode || '-'}</Descriptions.Item>
                  <Descriptions.Item label="binlog 保留">{cdcPrecheck.binlogRetention || '-'}</Descriptions.Item>
                  {cdcPrecheck.checks?.map(item => (
                    <Descriptions.Item key={item.code} label={item.label}>
                      <Tag color={item.passed ? 'success' : item.required ? 'error' : 'warning'}>
                        {item.passed ? '通过' : '未通过'}
                      </Tag>{' '}
                      {item.message}
                      {item.suggestion ? `；处理建议：${item.suggestion}` : ''}
                    </Descriptions.Item>
                  ))}
                </Descriptions>
              }
            />
          )}
          {compatibilityResult && compatibilityVisible && (
            <Alert
              style={{ width: '100%' }}
              type={compatibilityResult.passed ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage(compatibilityResult.message, () => setCompatibilityVisible(false))}
              description={
                <Descriptions size="small" column={{ xs: 1, sm: 2 }}>
                  <Descriptions.Item label="源表">{compatibilityResult.sourceTable}</Descriptions.Item>
                  <Descriptions.Item label="目标表">{compatibilityResult.targetTable}</Descriptions.Item>
                  {compatibilityResult.checks?.map(item => (
                    <Descriptions.Item key={item.code} label={item.label}>
                      <Tag color={item.passed ? 'success' : item.required ? 'error' : 'warning'}>
                        {item.passed ? '通过' : '未通过'}
                      </Tag>{' '}
                      {item.actual || '-'} {item.message}
                      {item.suggestion ? `；处理建议：${item.suggestion}` : ''}
                    </Descriptions.Item>
                  ))}
                </Descriptions>
              }
            />
          )}
          {validationResult && validationVisible && (
            <Alert
              style={{ width: '100%' }}
              type={validationResult.valid ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage(validationResult.message, () => setValidationVisible(false))}
              description={
                <Descriptions size="small" column={{ xs: 1, sm: 2 }}>
                  <Descriptions.Item label="源连接">
                    {validationResult.source.success ? '通过' : validationResult.source.message}
                  </Descriptions.Item>
                  <Descriptions.Item label="目标连接">
                    {validationResult.target.success ? '通过' : validationResult.target.message}
                  </Descriptions.Item>
                  {validationResult.cdcPrecheck && (
                    <Descriptions.Item label="CDC 前置检查">
                      {validationResult.cdcPrecheck.passed ? '通过' : validationResult.cdcPrecheck.message}
                    </Descriptions.Item>
                  )}
                  {validationResult.targetCompatibility && (
                    <Descriptions.Item label="目标表兼容性">
                      {validationResult.targetCompatibility.passed
                        ? '通过'
                        : validationResult.targetCompatibility.message}
                    </Descriptions.Item>
                  )}
                </Descriptions>
              }
            />
          )}
          {checkVisible && checkResult && (
            <Alert
              style={{ width: '100%' }}
              type={checkResult.success && checkResult.matched ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage('数据核对结果', () => setCheckVisible(false))}
              description={
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Descriptions size="small" column={1}>
                    <Descriptions.Item label="源表">{checkResult.sourceTable}</Descriptions.Item>
                    <Descriptions.Item label="目标表">{checkResult.targetTable}</Descriptions.Item>
                    <Descriptions.Item label="源端行数">{checkResult.sourceRows ?? '-'}</Descriptions.Item>
                    <Descriptions.Item label="目标端行数">{checkResult.targetRows ?? '-'}</Descriptions.Item>
                    <Descriptions.Item label="差异">{checkResult.difference ?? '-'}</Descriptions.Item>
                    <Descriptions.Item label="核对模式">
                      {checkResult.checkMode === 'KEY_RANGE'
                        ? `同步键分块（步长 ${checkResult.blockSize ?? '-'}）`
                        : '整表行数'}
                    </Descriptions.Item>
                    <Descriptions.Item label="分块统计">
                      {checkResult.totalBlocks == null
                        ? '-'
                        : `共 ${checkResult.totalBlocks} 块，通过 ${checkResult.matchedBlocks ?? 0}，不一致 ${checkResult.mismatchedBlocks ?? 0}，失败 ${checkResult.failedBlocks ?? 0}`}
                    </Descriptions.Item>
                    <Descriptions.Item label="结论">
                      <Tag color={checkResult.success && checkResult.matched ? 'success' : 'error'}>
                        {checkResult.message}
                      </Tag>
                    </Descriptions.Item>
                    <Descriptions.Item label="水位提示">
                      {checkResult.watermarkMessage ||
                        (detail.syncMode === 'FULL_CDC'
                          ? '严格核对要求任务已暂停；当前结果不代表持续 CDC 的实时同水位一致性。'
                          : '静态任务可作为当前快照核对结果。')}
                    </Descriptions.Item>
                    <Descriptions.Item label="记录时间">{new Date().toLocaleString()}</Descriptions.Item>
                  </Descriptions>
                  {checkResult.blocks && checkResult.blocks.length > 0 && (
                    <Table
                      size="small"
                      pagination={{ pageSize: 5 }}
                      rowKey="index"
                      dataSource={checkResult.blocks}
                      columns={[
                        { title: '块', dataIndex: 'index', width: 55 },
                        { title: '范围', render: (_, block) => `[${block.lowerBound}, ${block.upperBound})` },
                        { title: '源端', dataIndex: 'sourceRows' },
                        { title: '目标端', dataIndex: 'targetRows' },
                        { title: '差异', dataIndex: 'difference' },
                        {
                          title: '结果',
                          render: (_, block) => (
                            <Tag color={block.success && block.matched ? 'success' : 'error'}>
                              {block.message || (block.matched ? '一致' : '不一致')}
                            </Tag>
                          )
                        }
                      ]}
                    />
                  )}
                </Space>
              }
            />
          )}
          {previewVisible && (
            <Alert
              style={{ width: '100%' }}
              type="success"
              showIcon
              message={diagnosticMessage(previewTitle, () => setPreviewVisible(false))}
              description={
                <Typography.Paragraph
                  copyable={{ text: previewText }}
                  style={{
                    width: '100%',
                    maxHeight: 360,
                    overflow: 'auto',
                    whiteSpace: 'pre-wrap',
                    fontFamily: 'monospace',
                    marginBottom: 0
                  }}
                >
                  {previewText || '暂无配置内容'}
                </Typography.Paragraph>
              }
            />
          )}
          <Divider titlePlacement="left" plain>
            运行控制
          </Divider>
          <Space wrap>
            {permissions.canStart && ['DRAFT', 'STOPPED', 'FINISHED'].includes(taskStatus) && (
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                loading={busyAction === 'start'}
                disabled={actionDisabled}
                onClick={() => runAction('start', startSyncTask)}
              >
                启动任务
              </Button>
            )}
            {permissions.canResume && ['PAUSED', 'FAILED'].includes(taskStatus) && (
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                loading={busyAction === 'resume'}
                disabled={actionDisabled}
                onClick={() => runAction('resume', resumeSyncTask)}
              >
                恢复任务
              </Button>
            )}
            {permissions.canReinitialize && ['REINITIALIZE_REQUIRED', 'FAILED'].includes(taskStatus) && (
              <Button
                type="primary"
                danger
                icon={<ReloadOutlined />}
                loading={busyAction === 'reinitialize'}
                disabled={actionDisabled}
                onClick={() =>
                  Modal.confirm({
                    title: '重新初始化同步任务',
                    content:
                      detail.syncMode === 'FULL_CDC'
                        ? '此操作会丢弃旧 checkpoint/savepoint，重新执行全量并继续 CDC。目标端将按当前全量数据模式处理，是否继续？'
                        : '此操作会丢弃旧恢复状态并重新执行全量，是否继续？',
                    okText: '确认重新初始化',
                    cancelText: '取消',
                    onOk: () => runAction('reinitialize', reinitializeSyncTask)
                  })
                }
              >
                重新初始化
              </Button>
            )}
            {permissions.canPause && taskStatus === 'RUNNING' && (
              <Button
                icon={<PauseCircleOutlined />}
                loading={busyAction === 'pause'}
                disabled={actionDisabled}
                onClick={() => runAction('pause', pauseSyncTask, ['PAUSING'])}
              >
                暂停任务
              </Button>
            )}
            {permissions.canStop &&
              ['RUNNING', 'PAUSING', 'PAUSED', 'FAILED'].includes(taskStatus) &&
              detail.engineJobId && (
                <Button
                  danger
                  icon={<StopOutlined />}
                  loading={busyAction === 'stop'}
                  disabled={actionDisabled}
                  onClick={() =>
                    Modal.confirm({
                      title: '中止同步任务',
                      content: '中止后不会保留可恢复状态，是否继续？',
                      okText: '确认中止',
                      cancelText: '取消',
                      onOk: () => runAction('stop', stopSyncTask)
                    })
                  }
                >
                  中止任务
                </Button>
              )}
          </Space>
          <Divider titlePlacement="left" plain>
            配置管理
          </Divider>
          <Space wrap>
            {permissions.canEdit && (
              <Button
                icon={<EditOutlined />}
                disabled={actionDisabled || configLocked}
                title={configLocked ? '任务运行中不可修改' : undefined}
                onClick={() => {
                  close();
                  onEdit(detail);
                }}
              >
                修改任务
              </Button>
            )}
            {permissions.canRemove && (
              <Button
                danger
                icon={<DeleteOutlined />}
                loading={busyAction === 'delete'}
                disabled={actionDisabled || configLocked}
                title={configLocked ? '任务运行中不可删除' : undefined}
                onClick={() =>
                  Modal.confirm({
                    title: '删除同步任务',
                    content: `是否确认删除任务“${detail.taskName}”？`,
                    okText: '确认删除',
                    cancelText: '取消',
                    onOk: remove
                  })
                }
              >
                删除任务
              </Button>
            )}
          </Space>
        </Space>
      )}
      {detailLoading && <Typography.Text type="secondary">正在加载最新任务信息...</Typography.Text>}
    </Modal>
  );
}
