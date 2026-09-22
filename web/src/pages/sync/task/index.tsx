import { DeleteOutlined, EyeOutlined, PlusOutlined, StopOutlined } from '@ant-design/icons';
import { PageContainer, ProTable, type ActionType, type ProColumns } from '@ant-design/pro-components';
import { Button, message, Modal, Space, Tooltip } from 'antd';
import { useEffect, useRef, useState } from 'react';
import type { DataSourceVO } from '@/api/sync/data-source/types';
import type { SyncTaskQuery, SyncTaskVO } from '@/api/sync/task/types';
import { listDataSources } from '@/api/sync/data-source';
import { deleteSyncTask, listSyncTasks, stopSyncTask } from '@/api/sync/task';
import { useTableScroll } from '@/hooks/useTableScroll';
import TaskDetailModal from '@/pages/sync/task/components/TaskDetailModal';
import TaskFormModal from '@/pages/sync/task/components/TaskFormModal';
import { statusLabels, statusTag, useTaskPermissions } from '@/pages/sync/task/shared';
import { toPageQuery, toTableData } from '@/utils/ruoyi';

/** Task list. The wizard and the per-task console live in their own components. */
export default function SyncTaskPage() {
  const actionRef = useRef<ActionType | undefined>(undefined);
  const { tableScroll } = useTableScroll(1080);
  const permissions = useTaskPermissions();
  const [dataSources, setDataSources] = useState<DataSourceVO[]>([]);
  const [formOpen, setFormOpen] = useState(false);
  const [editingTask, setEditingTask] = useState<SyncTaskVO>();
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailTask, setDetailTask] = useState<SyncTaskVO>();
  const [busyRowAction, setBusyRowAction] = useState<string>();

  useEffect(() => {
    listDataSources({ pageNum: 1, pageSize: 100 }).then(res => setDataSources(res.data?.rows || []));
  }, []);

  const reload = () => actionRef.current?.reload();

  const openAdd = () => {
    setEditingTask(undefined);
    setFormOpen(true);
  };

  const openEdit = (row: SyncTaskVO) => {
    setEditingTask(row);
    setFormOpen(true);
  };

  const openDetail = (row: SyncTaskVO) => {
    setDetailTask(row);
    setDetailOpen(true);
  };

  const runRowAction = async (
    row: SyncTaskVO,
    action: 'stop' | 'delete',
    request: (id: string | number) => Promise<{ data?: unknown }>
  ) => {
    if (busyRowAction) return;
    setBusyRowAction(`${row.taskId}:${action}`);
    try {
      const result = await request(row.taskId);
      const resultMessage = (result.data as { message?: string } | undefined)?.message;
      message.success(resultMessage || (action === 'stop' ? '任务已中止' : '删除成功'));
      reload();
    } finally {
      setBusyRowAction(undefined);
    }
  };

  const rowActionDisabled = Boolean(busyRowAction);

  const columns: ProColumns<SyncTaskVO>[] = [
    { title: '任务名称', dataIndex: 'taskName', width: 190 },
    { title: '源表', dataIndex: 'sourceTable', width: 150, ellipsis: true },
    { title: '目标表', dataIndex: 'targetTable', width: 150, ellipsis: true },
    {
      title: '同步模式',
      dataIndex: 'syncMode',
      width: 140,
      search: false,
      valueEnum: { FULL: '全量', INCREMENTAL: '纯增量', FULL_CDC: '全量 + CDC' }
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 110,
      valueEnum: statusLabels,
      render: (_, row) => statusTag(row.status, row.lastError)
    },
    {
      title: '吞吐 / 积压',
      dataIndex: 'latestMetrics',
      width: 160,
      search: false,
      render: (_, row) =>
        row.latestMetrics ? (
          <Tooltip
            title={`采样 ${row.latestMetrics.sampledAt}${row.latestMetrics.cdcLagSeconds == null ? '' : `，端到端延迟 ${row.latestMetrics.cdcLagSeconds} 秒`}`}
          >
            {row.latestMetrics.sinkQps == null
              ? '-'
              : `${row.latestMetrics.sinkQps < 10 ? row.latestMetrics.sinkQps.toFixed(1) : Math.round(row.latestMetrics.sinkQps)} 行/秒`}{' '}
            · 积压 {row.latestMetrics.backlogRows ?? '-'}
          </Tooltip>
        ) : (
          '-'
        )
    },
    { title: '更新时间', dataIndex: 'updateTime', width: 170, search: false },
    {
      title: '操作',
      valueType: 'option',
      width: 240,
      fixed: 'right',
      render: (_, row) => {
        const canAbort =
          permissions.canStop &&
          ['RUNNING', 'PAUSING', 'PAUSED'].includes(row.status || '') &&
          Boolean(row.engineJobId);
        const deleteDisabled = ['RUNNING', 'PAUSING'].includes(row.status || '');
        return (
          <Space size={0} wrap={false} style={{ gap: 0, whiteSpace: 'nowrap' }}>
            {permissions.canDetail && (
              <Button
                type="link"
                style={{ paddingInline: 4 }}
                icon={<EyeOutlined />}
                disabled={rowActionDisabled}
                onClick={() => openDetail(row)}
              >
                详情
              </Button>
            )}
            {canAbort && (
              <Button
                type="link"
                danger
                style={{ paddingInline: 4 }}
                icon={<StopOutlined />}
                loading={busyRowAction === `${row.taskId}:stop`}
                disabled={rowActionDisabled}
                onClick={() =>
                  Modal.confirm({
                    title: '中止同步任务',
                    content: `中止后任务将停止运行，是否确认中止“${row.taskName}”？`,
                    okText: '确认中止',
                    cancelText: '取消',
                    onOk: () => runRowAction(row, 'stop', stopSyncTask)
                  })
                }
              >
                中止
              </Button>
            )}
            {permissions.canRemove && (
              <Button
                type="link"
                danger
                style={{ paddingInline: 4 }}
                icon={<DeleteOutlined />}
                loading={busyRowAction === `${row.taskId}:delete`}
                disabled={rowActionDisabled || deleteDisabled}
                title={deleteDisabled ? '任务运行中不可删除' : undefined}
                onClick={() =>
                  Modal.confirm({
                    title: '删除同步任务',
                    content: `是否确认删除任务“${row.taskName}”？`,
                    okText: '确认删除',
                    cancelText: '取消',
                    onOk: () => runRowAction(row, 'delete', deleteSyncTask)
                  })
                }
              >
                删除
              </Button>
            )}
          </Space>
        );
      }
    }
  ];

  return (
    <PageContainer title="同步任务">
      <ProTable<SyncTaskVO, SyncTaskQuery>
        actionRef={actionRef}
        rowKey="taskId"
        columns={columns}
        scroll={tableScroll}
        search={{ labelWidth: 80 }}
        pagination={{ defaultPageSize: 10, showSizeChanger: true }}
        request={async params => toTableData(await listSyncTasks(toPageQuery(params)))}
        toolbar={{ title: '同步任务列表' }}
        toolBarRender={() => [
          permissions.canAdd && (
            <Button key="add" type="primary" icon={<PlusOutlined />} onClick={openAdd}>
              新增任务
            </Button>
          )
        ]}
      />
      <TaskDetailModal
        open={detailOpen}
        task={detailTask}
        dataSources={dataSources}
        onClose={() => setDetailOpen(false)}
        onChanged={reload}
        onEdit={openEdit}
      />
      <TaskFormModal
        open={formOpen}
        task={editingTask}
        dataSources={dataSources}
        onClose={() => setFormOpen(false)}
        onSaved={reload}
      />
    </PageContainer>
  );
}
