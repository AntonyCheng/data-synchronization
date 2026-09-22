import { DeleteOutlined, EyeOutlined, PlusOutlined } from '@ant-design/icons';
import { PageContainer, ProTable, type ActionType, type ProColumns } from '@ant-design/pro-components';
import { Button, message, Modal, Space, Tag } from 'antd';
import { useEffect, useRef, useState } from 'react';
import type { DataSourceVO } from '@/api/sync/data-source/types';
import type { SyncTaskGroupQuery, SyncTaskGroupVO } from '@/api/sync/group/types';
import { listDataSources } from '@/api/sync/data-source';
import { deleteSyncTaskGroup, listSyncTaskGroups } from '@/api/sync/group';
import { useTableScroll } from '@/hooks/useTableScroll';
import GroupDetailModal from '@/pages/sync/group/components/GroupDetailModal';
import GroupFormModal from '@/pages/sync/group/components/GroupFormModal';
import { groupStatusLabel, groupStatusLabels, useGroupPermissions } from '@/pages/sync/group/shared';
import { toPageQuery, toTableData } from '@/utils/ruoyi';

/** Task group list. The form and the per-group console live in their own components. */
export default function SyncTaskGroupPage() {
  const actionRef = useRef<ActionType | undefined>(undefined);
  const { tableScroll } = useTableScroll(1000);
  const can = useGroupPermissions();
  const [dataSources, setDataSources] = useState<DataSourceVO[]>([]);
  const [formOpen, setFormOpen] = useState(false);
  const [editingGroup, setEditingGroup] = useState<SyncTaskGroupVO>();
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailGroup, setDetailGroup] = useState<SyncTaskGroupVO>();

  useEffect(() => {
    listDataSources({ pageNum: 1, pageSize: 100 }).then(res => setDataSources(res.data?.rows || []));
  }, []);

  const reload = () => actionRef.current?.reload();

  const openAdd = () => {
    setEditingGroup(undefined);
    setFormOpen(true);
  };

  const openEdit = (row: SyncTaskGroupVO) => {
    setEditingGroup(row);
    setFormOpen(true);
  };

  const openDetail = (row: SyncTaskGroupVO) => {
    setDetailGroup(row);
    setDetailOpen(true);
  };

  const remove = async (row: SyncTaskGroupVO) => {
    await deleteSyncTaskGroup(row.groupId);
    message.success('删除成功');
    actionRef.current?.reloadAndRest?.();
  };

  const columns: ProColumns<SyncTaskGroupVO>[] = [
    { title: '任务组', dataIndex: 'groupName', width: 220 },
    {
      title: '粒度',
      dataIndex: 'syncScope',
      width: 100,
      search: false,
      render: (_, row) => (
        <Tag color={row.syncScope === 'DATABASE' ? 'processing' : 'default'}>
          {row.syncScope === 'DATABASE' ? '整库' : '多表'}
        </Tag>
      )
    },
    { title: '表数量', dataIndex: 'items', width: 90, search: false, render: (_, row) => row.items?.length || 0 },
    { title: '版本', dataIndex: 'configVersion', width: 80, search: false },
    {
      title: '状态',
      dataIndex: 'status',
      width: 120,
      valueEnum: groupStatusLabels,
      render: (_, row) => (
        <Tag
          color={
            row.status === 'RUNNING' || row.status === 'VALID'
              ? 'success'
              : row.status === 'DEGRADED'
                ? 'warning'
                : row.status === 'FAILED' || row.status === 'INVALID'
                  ? 'error'
                  : 'default'
          }
        >
          {groupStatusLabel(row.status)}
        </Tag>
      )
    },
    {
      title: '操作',
      valueType: 'option',
      width: 170,
      fixed: 'right',
      render: (_, row) => {
        const live = ['RUNNING', 'PAUSING', 'DEGRADED'].includes(row.status || '');
        return (
          <Space size={6} wrap={false} className="group-row-actions-react">
            {can('sync:group:query') && (
              <Button type="link" size="small" icon={<EyeOutlined />} onClick={() => openDetail(row)}>
                详情
              </Button>
            )}
            {can('sync:group:remove') && (
              <Button
                type="link"
                size="small"
                danger
                icon={<DeleteOutlined />}
                disabled={live}
                title={live ? '运行中的任务组不能删除' : undefined}
                onClick={() =>
                  Modal.confirm({
                    title: '删除同步任务组',
                    content: `是否确认删除任务组“${row.groupName}”？`,
                    okText: '确认删除',
                    cancelText: '取消',
                    onOk: () => remove(row)
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
    <PageContainer title="多表同步">
      <ProTable<SyncTaskGroupVO, SyncTaskGroupQuery>
        actionRef={actionRef}
        rowKey="groupId"
        columns={columns}
        scroll={tableScroll}
        search={{ labelWidth: 80 }}
        pagination={{ defaultPageSize: 10 }}
        request={async params => toTableData(await listSyncTaskGroups(toPageQuery(params)))}
        toolbar={{ title: '多表同步任务组' }}
        toolBarRender={() => [
          can('sync:group:add') && (
            <Button key="add" type="primary" icon={<PlusOutlined />} onClick={openAdd}>
              新增任务组
            </Button>
          )
        ]}
      />
      <GroupDetailModal
        open={detailOpen}
        group={detailGroup}
        dataSources={dataSources}
        onClose={() => setDetailOpen(false)}
        onChanged={reload}
        onEdit={openEdit}
      />
      <GroupFormModal
        open={formOpen}
        group={editingGroup}
        dataSources={dataSources}
        onClose={() => setFormOpen(false)}
        onSaved={reload}
      />
    </PageContainer>
  );
}
