import {
  CheckCircleOutlined,
  DeleteOutlined,
  EditOutlined,
  EyeOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
  StopOutlined
} from '@ant-design/icons';
import { Alert, Button, Descriptions, Divider, Input, message, Modal, Space, Tag } from 'antd';
import { useEffect, useState } from 'react';
import type { DataSourceOptionVO } from '@/api/sync/data-source/types';
import type { SyncTaskGroupDataCheckResult, SyncTaskGroupVO } from '@/api/sync/group/types';
import {
  checkSyncTaskGroupData,
  checkSyncTaskGroupDdl,
  deleteSyncTaskGroup,
  discoverSyncTaskGroupTables,
  getSyncTaskGroup,
  getSyncTaskGroupItemMetrics,
  pauseSyncTaskGroup,
  previewSyncTaskGroupConfig,
  refreshSyncTaskGroupStatus,
  reinitializeSyncTaskGroupItem,
  resumeSyncTaskGroup,
  resumeSyncTaskGroupItemAfterDdl,
  startSyncTaskGroup,
  stopSyncTaskGroup,
  validateSyncTaskGroup
} from '@/api/sync/group';
import MetricsHistoryPanel from '@/components/sync/MetricsHistoryPanel';
import {
  groupStatusLabel,
  itemLabel,
  itemStatusColor,
  itemStatusLabels,
  kafkaOutputFormatLabel,
  reinitializableItemStatuses,
  sourceTypeOf,
  useGroupPermissions
} from '@/pages/sync/group/shared';

export interface GroupDetailModalProps {
  open: boolean;
  /** The clicked row; the modal reloads it from the server and keeps the fresh copy. */
  group?: SyncTaskGroupVO;
  dataSources: DataSourceOptionVO[];
  onClose: () => void;
  /** The group changed server-side (action, discover, delete) - the list should reload. */
  onChanged: () => void;
  onEdit: (group: SyncTaskGroupVO) => void;
}

/**
 * The per-group console: run control, config/discovery actions, DDL and validation
 * results, data check, and the per-table status block with its table-level rebuild.
 */
export default function GroupDetailModal({
  open,
  group,
  dataSources,
  onClose,
  onChanged,
  onEdit
}: GroupDetailModalProps) {
  const can = useGroupPermissions();
  const [detail, setDetail] = useState<SyncTaskGroupVO | undefined>(group);
  const [dataCheckResult, setDataCheckResult] = useState<SyncTaskGroupDataCheckResult>();
  const [validationResult, setValidationResult] = useState<Awaited<ReturnType<typeof validateSyncTaskGroup>>['data']>();
  const [configPreview, setConfigPreview] = useState<Awaited<ReturnType<typeof previewSyncTaskGroupConfig>>['data']>();
  const [ddlResult, setDdlResult] = useState<Awaited<ReturnType<typeof checkSyncTaskGroupDdl>>['data']>();
  const [metricsItem, setMetricsItem] = useState<{ itemId: string | number; sourceTable?: string }>();

  const groupId = group?.groupId;

  useEffect(() => {
    if (!open || !groupId) return;
    setDetail(group);
    setDataCheckResult(undefined);
    setValidationResult(undefined);
    setConfigPreview(undefined);
    setDdlResult(undefined);
    void getSyncTaskGroup(groupId).then(result => setDetail(result.data));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, groupId]);

  const reloadDetail = async (id: string | number) => {
    const latest = await getSyncTaskGroup(id);
    setDetail(latest.data);
    onChanged();
    return latest.data;
  };

  const operate = async (action: 'start' | 'status' | 'pause' | 'resume' | 'stop') => {
    if (!detail) return;
    const request = {
      start: startSyncTaskGroup,
      status: refreshSyncTaskGroupStatus,
      pause: pauseSyncTaskGroup,
      resume: resumeSyncTaskGroup,
      stop: stopSyncTaskGroup
    }[action];
    // A refused action still moves the group (a failed start isolates its tables), so the
    // console reloads either way - otherwise it keeps offering buttons for a stale status.
    try {
      const result = await request(detail.groupId);
      message.success(result.data.message);
    } finally {
      await reloadDetail(detail.groupId);
    }
  };

  const discover = async () => {
    if (!detail) return;
    try {
      const result = await discoverSyncTaskGroupTables(detail.groupId);
      message.success(result.data.message);
    } finally {
      await reloadDetail(detail.groupId);
    }
  };

  const validate = async () => {
    if (!detail) return;
    // Validation is a read-only check and never changes the group's actual lifecycle
    // status - overwriting detail.status with a synthetic 'VALID'/'INVALID' here (as this
    // used to) desynced the displayed status and run-control buttons from backend truth.
    const result = await validateSyncTaskGroup(detail.groupId);
    setValidationResult(result.data);
  };

  const preview = async () => {
    if (!detail) return;
    const result = await previewSyncTaskGroupConfig(detail.groupId);
    setConfigPreview(result.data);
  };

  const ddlCheck = async (id?: string | number) => {
    const target = id ?? detail?.groupId;
    if (!target) return;
    const result = await checkSyncTaskGroupDdl(target);
    setDdlResult(result.data);
  };

  const checkData = async () => {
    if (!detail) return;
    try {
      const result = await checkSyncTaskGroupData(detail.groupId);
      setDataCheckResult(result.data);
      message.success(result.data.message);
    } finally {
      await reloadDetail(detail.groupId);
    }
  };

  const remove = async () => {
    if (!detail) return;
    await deleteSyncTaskGroup(detail.groupId);
    message.success('删除成功');
    onClose();
    onChanged();
  };

  /** Table-level rebuild: the way out of an isolated table whose savepoint can no longer be reused. */
  const reinitializeItem = (itemId: string | number, sourceTable?: string) =>
    Modal.confirm({
      title: `重新初始化表 ${sourceTable || itemId}`,
      content:
        '将丢弃该表的作业与 savepoint 并重新全量同步；其他表不受影响。若启动时选择的是全部字段，源表新增的字段会一并纳入。是否继续？',
      okText: '确认重新初始化',
      cancelText: '取消',
      onOk: async () => {
        if (!detail) return;
        try {
          const result = await reinitializeSyncTaskGroupItem(detail.groupId, itemId);
          message.success(result.data.message);
          if (ddlResult) await ddlCheck(detail.groupId);
        } finally {
          await reloadDetail(detail.groupId);
        }
      }
    });

  const targetType = sourceTypeOf(dataSources, detail?.targetId);
  const status = detail?.status || '';
  const liveStatuses = ['RUNNING', 'PAUSING', 'DEGRADED'];

  return (
    <>
      <Modal
        title={detail ? `任务组详情：${detail.groupName}` : '任务组详情'}
        open={open}
        width={1040}
        footer={null}
        destroyOnHidden
        onCancel={onClose}
      >
        {detail && (
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }}>
              <Descriptions.Item label="任务组状态">{groupStatusLabel(detail.status)}</Descriptions.Item>
              <Descriptions.Item label="同步粒度">
                {detail.syncScope === 'DATABASE' ? '整库' : '多表'}
              </Descriptions.Item>
              <Descriptions.Item label="表数量">{detail.items.length}</Descriptions.Item>
              <Descriptions.Item label="同步模式">
                {{ FULL: '全量', INCREMENTAL: '增量', FULL_CDC: '全量 + CDC' }[detail.syncMode || ''] ||
                  detail.syncMode ||
                  '-'}
              </Descriptions.Item>
              {targetType === 'KAFKA' && (
                <Descriptions.Item label="Kafka 输出格式">
                  {kafkaOutputFormatLabel(detail.kafkaOutputFormat)}
                </Descriptions.Item>
              )}
              <Descriptions.Item label="配置版本">{detail.configVersion || '-'}</Descriptions.Item>
              <Descriptions.Item label="最近检查点">{detail.lastCheckpointTime || '-'}</Descriptions.Item>
              <Descriptions.Item label="每表最大行数/秒">{detail.readLimitRowsPerSecond ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="每表最大字节/秒">{detail.readLimitBytesPerSecond ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="每表快照并行度">{detail.snapshotParallelism ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="每表 CDC 连接池">{detail.sourceConnectionLimit ?? '-'}</Descriptions.Item>
            </Descriptions>
            <Divider titlePlacement="left" plain>
              运行控制
            </Divider>
            <Space wrap size={8}>
              {can('sync:group:start') && (
                <Button
                  type="primary"
                  icon={<PlayCircleOutlined />}
                  disabled={['RUNNING', 'PAUSING', 'PAUSED'].includes(status)}
                  onClick={() => operate('start')}
                >
                  启动
                </Button>
              )}
              {can('sync:group:status') && (
                <Button icon={<ReloadOutlined />} onClick={() => operate('status')}>
                  刷新状态
                </Button>
              )}
              {can('sync:group:pause') && (
                <Button
                  icon={<PauseCircleOutlined />}
                  disabled={!['RUNNING', 'DEGRADED'].includes(status)}
                  onClick={() => operate('pause')}
                >
                  暂停
                </Button>
              )}
              {can('sync:group:resume') && (
                <Button
                  icon={<PlayCircleOutlined />}
                  disabled={!['PAUSED', 'FAILED'].includes(status)}
                  onClick={() => operate('resume')}
                >
                  恢复
                </Button>
              )}
              {can('sync:group:stop') && (
                <Button
                  danger
                  icon={<StopOutlined />}
                  disabled={!['RUNNING', 'PAUSING', 'PAUSED', 'FAILED', 'DEGRADED'].includes(status)}
                  onClick={() =>
                    Modal.confirm({
                      title: '中止同步任务组',
                      content: '中止后不会保留可恢复状态，是否继续？',
                      okText: '确认中止',
                      cancelText: '取消',
                      onOk: () => operate('stop')
                    })
                  }
                >
                  中止
                </Button>
              )}
            </Space>
            <Divider titlePlacement="left" plain>
              配置与发现
            </Divider>
            <Space wrap size={8}>
              {can('sync:group:edit') && (
                <Button
                  icon={<EditOutlined />}
                  disabled={liveStatuses.includes(status)}
                  onClick={() => {
                    onClose();
                    onEdit(detail);
                  }}
                >
                  修改任务组
                </Button>
              )}
              {can('sync:group:discover') && detail.syncScope === 'DATABASE' && (
                <Button icon={<ReloadOutlined />} onClick={discover}>
                  扫描新表
                </Button>
              )}
              {can('sync:group:engine-config') && (
                <Button icon={<EyeOutlined />} onClick={preview}>
                  配置预览
                </Button>
              )}
              {can('sync:group:validate') && (
                <Button icon={<SafetyCertificateOutlined />} onClick={validate}>
                  连接与配置校验
                </Button>
              )}
              {can('sync:group:ddl-check') && (
                <Button icon={<SafetyCertificateOutlined />} onClick={() => ddlCheck()}>
                  结构检查
                </Button>
              )}
            </Space>
            {validationResult && (
              <Alert
                type={validationResult.valid ? 'success' : 'error'}
                showIcon
                closable
                onClose={() => setValidationResult(undefined)}
                message={validationResult.message}
                description={
                  <Descriptions size="small" column={1}>
                    <Descriptions.Item label="源连接">{validationResult.source.message}</Descriptions.Item>
                    <Descriptions.Item label="目标连接">{validationResult.target.message}</Descriptions.Item>
                    {validationResult.items.map(item => (
                      <Descriptions.Item key={String(item.itemId)} label={`${item.sourceTable} → ${item.targetTable}`}>
                        <Tag color={item.passed ? 'success' : 'error'}>{item.passed ? '通过' : '失败'}</Tag>{' '}
                        {item.message}
                      </Descriptions.Item>
                    ))}
                  </Descriptions>
                }
              />
            )}
            {configPreview && (
              <Alert
                type="success"
                showIcon
                closable
                onClose={() => setConfigPreview(undefined)}
                message={`${configPreview.groupName} 配置预览`}
                description={
                  <Input.TextArea
                    value={configPreview.config}
                    readOnly
                    autoSize={{ minRows: 12, maxRows: 24 }}
                    style={{ width: '100%', resize: 'vertical' }}
                  />
                }
              />
            )}
            {ddlResult && (
              <Alert
                type={ddlResult.events.length ? 'warning' : 'success'}
                showIcon
                closable
                onClose={() => setDdlResult(undefined)}
                message={ddlResult.message}
                description={
                  ddlResult.events.length === 0 ? undefined : (
                    <Space direction="vertical" style={{ width: '100%' }} size={12}>
                      {ddlResult.events.map(event => (
                        <Descriptions
                          key={String(event.eventId)}
                          size="small"
                          bordered
                          column={1}
                          title={
                            <Space>
                              <span>
                                {event.sourceTable} → {event.targetTable}
                              </span>
                              <Tag color={event.riskLevel === 'HIGH' ? 'error' : 'warning'}>
                                {event.riskLevel === 'HIGH' ? '高风险' : '低风险'}
                              </Tag>
                              <Tag color={event.status === 'READY_TO_RESUME' ? 'success' : 'error'}>
                                {event.status === 'READY_TO_RESUME' ? '可恢复' : '待修复'}
                              </Tag>
                            </Space>
                          }
                        >
                          <Descriptions.Item label="变更类型">{event.changeType}</Descriptions.Item>
                          <Descriptions.Item label="变更详情">{event.details}</Descriptions.Item>
                          <Descriptions.Item label="处理建议">{event.remediation}</Descriptions.Item>
                          <Descriptions.Item label="操作">
                            <Space wrap size={8}>
                              {event.status === 'READY_TO_RESUME' ? (
                                <Button
                                  type="primary"
                                  size="small"
                                  onClick={async () => {
                                    await resumeSyncTaskGroupItemAfterDdl(detail.groupId, event.itemId);
                                    message.success('表项已恢复');
                                    await ddlCheck(detail.groupId);
                                  }}
                                >
                                  恢复该表
                                </Button>
                              ) : (
                                <span>修复目标表后重新执行检查</span>
                              )}
                              {can('sync:group:reinitialize') && (
                                <Button
                                  size="small"
                                  danger
                                  onClick={() => reinitializeItem(event.itemId, event.sourceTable)}
                                >
                                  重新初始化该表
                                </Button>
                              )}
                            </Space>
                          </Descriptions.Item>
                        </Descriptions>
                      ))}
                    </Space>
                  )
                }
              />
            )}
            <Divider titlePlacement="left" plain>
              危险操作
            </Divider>
            <Button
              danger
              icon={<DeleteOutlined />}
              disabled={liveStatuses.includes(status)}
              title={liveStatuses.includes(status) ? '运行中的任务组不能删除' : undefined}
              onClick={() =>
                Modal.confirm({
                  title: '删除同步任务组',
                  content: `是否确认删除任务组“${detail.groupName}”？`,
                  okText: '确认删除',
                  cancelText: '取消',
                  onOk: remove
                })
              }
            >
              删除任务组
            </Button>
            <Divider titlePlacement="left" plain>
              数据质量核对
            </Divider>
            {can('sync:group:check') && (
              <Button icon={<CheckCircleOutlined />} onClick={checkData}>
                执行逐表核对
              </Button>
            )}
            {dataCheckResult && (
              <Alert
                type={dataCheckResult.matched ? 'success' : 'warning'}
                showIcon
                message={dataCheckResult.message}
                description={
                  <Descriptions size="small" column={{ xs: 1, sm: 2, md: 4 }}>
                    <Descriptions.Item label="一致表">{dataCheckResult.matchedTableCount}</Descriptions.Item>
                    <Descriptions.Item label="不一致表">{dataCheckResult.mismatchedTableCount}</Descriptions.Item>
                    <Descriptions.Item label="执行失败">{dataCheckResult.failedTableCount}</Descriptions.Item>
                    <Descriptions.Item label="核对范围">{dataCheckResult.tableCount} 张表</Descriptions.Item>
                    <Descriptions.Item label="说明" span={4}>
                      {dataCheckResult.consistencyNote}
                    </Descriptions.Item>
                  </Descriptions>
                }
              />
            )}
            <Descriptions bordered size="small" column={1} title="表项状态">
              {detail.items.map(item => (
                <Descriptions.Item key={String(item.itemId)} label={itemLabel(item, targetType)}>
                  <Space wrap size={8}>
                    <Tag color={itemStatusColor(item.status)}>
                      {item.status ? itemStatusLabels[item.status] || item.status : '-'}
                    </Tag>
                    {item.engineJobId && <span>作业 {item.engineJobId}</span>}
                    {item.latestMetrics && (
                      <span>
                        {item.latestMetrics.sinkQps == null
                          ? '-'
                          : `${item.latestMetrics.sinkQps < 10 ? item.latestMetrics.sinkQps.toFixed(1) : Math.round(item.latestMetrics.sinkQps)} 行/秒`}{' '}
                        · 积压 {item.latestMetrics.backlogRows ?? '-'}
                        {item.latestMetrics.cdcLagSeconds != null && ` · 延迟 ${item.latestMetrics.cdcLagSeconds} 秒`}
                      </span>
                    )}
                    {can('sync:group:status') && (
                      <Button
                        size="small"
                        onClick={() => setMetricsItem({ itemId: item.itemId, sourceTable: item.sourceTable })}
                      >
                        指标历史
                      </Button>
                    )}
                    {item.lastError && <span style={{ color: '#cf1322' }}>{item.lastError}</span>}
                    {can('sync:group:reinitialize') &&
                      reinitializableItemStatuses.includes(item.status || '') &&
                      !['DRAFT', 'PAUSING'].includes(status) && (
                        <Button size="small" danger onClick={() => reinitializeItem(item.itemId, item.sourceTable)}>
                          重新初始化该表
                        </Button>
                      )}
                  </Space>
                </Descriptions.Item>
              ))}
            </Descriptions>
            <Descriptions bordered size="small" column={1} title="逐表最近核对结果">
              {detail.items.map(item => (
                <Descriptions.Item key={String(item.itemId)} label={itemLabel(item, targetType)}>
                  {item.lastCheckTime ? (
                    <Space wrap size={8}>
                      <Tag color={item.lastCheckMatched === '1' ? 'success' : 'error'}>
                        {item.lastCheckMatched === '1' ? '行数一致' : '未一致 / 失败'}
                      </Tag>
                      <span>
                        源 {item.lastCheckSourceRows ?? '-'}，目标 {item.lastCheckTargetRows ?? '-'}，差异{' '}
                        {item.lastCheckDifference ?? '-'}
                      </span>
                      <span>{item.lastCheckTime}</span>
                      <span>{item.lastCheckMessage}</span>
                    </Space>
                  ) : (
                    <Tag>尚未核对</Tag>
                  )}
                </Descriptions.Item>
              ))}
            </Descriptions>
          </Space>
        )}
      </Modal>
      <Modal
        title={metricsItem ? `运行指标历史：${metricsItem.sourceTable || metricsItem.itemId}` : '运行指标历史'}
        open={Boolean(metricsItem)}
        width={880}
        footer={null}
        destroyOnHidden
        onCancel={() => setMetricsItem(undefined)}
      >
        {detail && metricsItem && (
          <MetricsHistoryPanel
            load={minutes =>
              getSyncTaskGroupItemMetrics(detail.groupId, metricsItem.itemId, minutes).then(result => result.data)
            }
          />
        )}
      </Modal>
    </>
  );
}
