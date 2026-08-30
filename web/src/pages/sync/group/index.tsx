import { CheckCircleOutlined, DeleteOutlined, EditOutlined, EyeOutlined, PauseCircleOutlined, PlayCircleOutlined, PlusOutlined, ReloadOutlined, SafetyCertificateOutlined, StopOutlined } from '@ant-design/icons';
import { ModalForm, PageContainer, ProFormDigit, ProFormRadio, ProFormSelect, ProFormSwitch, ProFormText, ProTable, type ActionType, type ProColumns } from '@ant-design/pro-components';
import { Alert, Button, Checkbox, Descriptions, Divider, Form, Input, message, Modal, Select, Space, Tag } from 'antd';
import { useEffect, useRef, useState } from 'react';
import { getDataSourceMetadata, listDataSourceTables, listDataSources } from '@/api/sync/data-source';
import type { DataSourceMetadataVO, DataSourceVO } from '@/api/sync/data-source/types';
import { addSyncTaskGroup, checkSyncTaskGroupData, checkSyncTaskGroupDdl, deleteSyncTaskGroup, discoverSyncTaskGroupTables, getSyncTaskGroup, listSyncTaskGroups, pauseSyncTaskGroup, previewSyncTaskGroupConfig, refreshSyncTaskGroupStatus, resumeSyncTaskGroup, resumeSyncTaskGroupItemAfterDdl, startSyncTaskGroup, stopSyncTaskGroup, updateSyncTaskGroup, validateSyncTaskGroup } from '@/api/sync/group';
import type { SyncTaskGroupDataCheckResult, SyncTaskGroupForm, SyncTaskGroupQuery, SyncTaskGroupVO } from '@/api/sync/group/types';
import RowActions from '@/components/common/RowActions';
import { useTableScroll } from '@/hooks/useTableScroll';
import { useUserStore } from '@/stores/userStore';
import { hasPermi } from '@/utils/permission';
import { toPageQuery, toTableData } from '@/utils/ruoyi';

const emptyForm: SyncTaskGroupForm = { syncScope: 'MULTI_TABLE', autoDiscover: '0', syncMode: 'FULL_CDC', ddlPolicy: 'FAIL', readLimitRowsPerSecond: 1000, readLimitBytesPerSecond: 10485760, snapshotParallelism: 1, sourceConnectionLimit: 2, items: [{ targetSchema: 'public' }] };

const groupStatusLabels: Record<string, string> = {
  DRAFT: '草稿',
  RUNNING: '运行中',
  PAUSING: '暂停中',
  PAUSED: '已暂停',
  DEGRADED: '部分异常',
  STOPPED: '已停止',
  FAILED: '失败',
  FINISHED: '已完成',
  VALID: '校验通过',
  INVALID: '校验失败',
};

function groupStatusLabel(status?: string) {
  return status ? groupStatusLabels[status] || status : '草稿';
}

function keyOptions(metadata?: DataSourceMetadataVO) {
  if (!metadata) return [];
  const options = metadata.primaryKeys.length ? [{ label: `主键（${metadata.primaryKeys.join(', ')}）`, value: metadata.primaryKeys.join(',') }] : [];
  metadata.uniqueKeys.filter(key => key.allNotNull).forEach(key => options.push({ label: `唯一键 ${key.name}（${key.columns.join(', ')}）`, value: key.columns.join(',') }));
  return options;
}

export default function SyncTaskGroupPage() {
  const actionRef = useRef<ActionType | undefined>(undefined);
  const [form] = Form.useForm<SyncTaskGroupForm>();
  const { tableScroll } = useTableScroll(1000);
  const userInfo = useUserStore(state => state.userInfo);
  const [dataSources, setDataSources] = useState<DataSourceVO[]>([]);
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [itemMetadata, setItemMetadata] = useState<Record<string, DataSourceMetadataVO>>({});
  const [modalOpen, setModalOpen] = useState(false);
  const [modalTitle, setModalTitle] = useState('');
  const [detail, setDetail] = useState<SyncTaskGroupVO>();
  const [detailOpen, setDetailOpen] = useState(false);
  const [dataCheckResult, setDataCheckResult] = useState<SyncTaskGroupDataCheckResult>();
  const syncScope = Form.useWatch('syncScope', form) || 'MULTI_TABLE';

  const can = (permission: string) => hasPermi(userInfo, [permission]);
  const sourceOptions = dataSources.filter(item => item.sourceType === 'MYSQL').map(item => ({ label: `${item.sourceName} (${item.databaseName})`, value: item.sourceId }));
  const targetOptions = dataSources.filter(item => item.sourceType === 'POSTGRESQL').map(item => ({ label: `${item.sourceName} (${item.databaseName})`, value: item.sourceId }));

  useEffect(() => { listDataSources({ pageNum: 1, pageSize: 100 }).then(res => setDataSources(res.data?.rows || [])); }, []);

  const loadTables = async (sourceId?: string | number) => {
    const source = dataSources.find(item => String(item.sourceId) === String(sourceId));
    if (source) setSourceTables((await listDataSourceTables(source.sourceId, source.databaseName)).data || []);
  };

  const loadItemMetadata = async (itemIndex: number, tableName?: string, sourceId?: string | number) => {
    const source = dataSources.find(item => String(item.sourceId) === String(sourceId ?? form.getFieldValue('sourceId')));
    if (!source || !tableName) return;
    const result = await getDataSourceMetadata(source.sourceId, source.databaseName || '', tableName);
    setItemMetadata(current => ({ ...current, [String(itemIndex)]: result.data }));
    const selectedColumns = form.getFieldValue(['items', itemIndex, 'selectedColumns']);
    if (typeof selectedColumns === 'string') form.setFieldValue(['items', itemIndex, 'selectedColumns'], selectedColumns.split(',').filter(Boolean));
    else if (!selectedColumns) form.setFieldValue(['items', itemIndex, 'selectedColumns'], result.data.columns.map(column => column.name));
    if (!form.getFieldValue(['items', itemIndex, 'syncKeyColumns'])) form.setFieldValue(['items', itemIndex, 'syncKeyColumns'], keyOptions(result.data)[0]?.value);
  };

  const openAdd = () => { form.resetFields(); form.setFieldsValue(emptyForm); setItemMetadata({}); setModalTitle('新增多表同步任务'); setModalOpen(true); };
  const openEdit = async (row: SyncTaskGroupVO) => { const result = await getSyncTaskGroup(row.groupId); form.resetFields(); form.setFieldsValue(result.data); setItemMetadata({}); await loadTables(result.data.sourceId); await Promise.all((result.data.items || []).map((item, index) => loadItemMetadata(index, item.sourceTable, result.data.sourceId))); setModalTitle('修改多表同步任务'); setModalOpen(true); };
  const openDetail = async (row: SyncTaskGroupVO) => { const result = await getSyncTaskGroup(row.groupId); setDetail(result.data); setDataCheckResult(undefined); setDetailOpen(true); };
  const submit = async (values: SyncTaskGroupForm) => { const payload = { ...values, items: values.items?.map(item => ({ ...item, selectedColumns: Array.isArray(item.selectedColumns) ? item.selectedColumns.join(',') : item.selectedColumns })) }; if (values.groupId) await updateSyncTaskGroup(payload); else await addSyncTaskGroup(payload); message.success('保存成功'); setModalOpen(false); actionRef.current?.reload(); return true; };
  const remove = async (row: SyncTaskGroupVO) => { await deleteSyncTaskGroup(row.groupId); message.success('删除成功'); actionRef.current?.reloadAndRest?.(); };
  const validate = async (row: SyncTaskGroupVO) => { const result = await validateSyncTaskGroup(row.groupId); setDetail({ ...row, status: result.data.valid ? 'VALID' : 'INVALID' }); Modal.info({ title: '多表校验结果', width: 760, content: <Space direction="vertical" style={{ width: '100%' }}><Tag color={result.data.valid ? 'success' : 'error'}>{result.data.message}</Tag><Descriptions size="small" column={1}><Descriptions.Item label="源连接">{result.data.source.message}</Descriptions.Item><Descriptions.Item label="目标连接">{result.data.target.message}</Descriptions.Item>{result.data.items.map(item => <Descriptions.Item key={String(item.itemId)} label={`${item.sourceTable} → ${item.targetTable}`}><Tag color={item.passed ? 'success' : 'error'}>{item.passed ? '通过' : '失败'}</Tag> {item.message}</Descriptions.Item>)}</Descriptions></Space> }); };
  const preview = async (row: SyncTaskGroupVO) => { const result = await previewSyncTaskGroupConfig(row.groupId); Modal.info({ title: `${result.data.groupName} 配置预览`, width: 900, content: <Input.TextArea value={result.data.config} readOnly autoSize={{ minRows: 12, maxRows: 24 }} /> }); };
  const operate = async (row: SyncTaskGroupVO, action: 'start' | 'status' | 'pause' | 'resume' | 'stop') => {
    const request = { start: startSyncTaskGroup, status: refreshSyncTaskGroupStatus, pause: pauseSyncTaskGroup, resume: resumeSyncTaskGroup, stop: stopSyncTaskGroup }[action];
    const result = await request(row.groupId);
    message.success(result.data.message);
    actionRef.current?.reload();
  };
  const discover = async (row: SyncTaskGroupVO) => { const result = await discoverSyncTaskGroupTables(row.groupId); message.success(result.data.message); actionRef.current?.reload(); };
  const ddlCheck = async (row: SyncTaskGroupVO) => {
    const result = await checkSyncTaskGroupDdl(row.groupId);
    let modal: ReturnType<typeof Modal.info>;
    const refresh = async () => { modal?.destroy(); await ddlCheck(row); actionRef.current?.reload(); };
    modal = Modal.info({
      title: '表结构变更检查',
      width: 820,
      content: result.data.events.length === 0
        ? <Tag color="success">{result.data.message}</Tag>
        : <Space direction="vertical" style={{ width: '100%' }} size={12}>{result.data.events.map(event => <Descriptions key={String(event.eventId)} size="small" bordered column={1} title={<Space><span>{event.sourceTable} → {event.targetTable}</span><Tag color={event.riskLevel === 'HIGH' ? 'error' : 'warning'}>{event.riskLevel === 'HIGH' ? '高风险' : '低风险'}</Tag><Tag color={event.status === 'READY_TO_RESUME' ? 'success' : 'error'}>{event.status === 'READY_TO_RESUME' ? '可恢复' : '待修复'}</Tag></Space>}><Descriptions.Item label="变更类型">{event.changeType}</Descriptions.Item><Descriptions.Item label="变更详情">{event.details}</Descriptions.Item><Descriptions.Item label="处理建议">{event.remediation}</Descriptions.Item><Descriptions.Item label="操作">{event.status === 'READY_TO_RESUME' ? <Button type="primary" size="small" onClick={async () => { await resumeSyncTaskGroupItemAfterDdl(row.groupId, event.itemId); message.success('表项已恢复'); await refresh(); }}>恢复该表</Button> : <span>修复目标表后重新执行检查</span>}</Descriptions.Item></Descriptions>)}</Space>
    });
  };
  const checkData = async () => {
    if (!detail) return;
    const result = await checkSyncTaskGroupData(detail.groupId);
    setDataCheckResult(result.data);
    const latest = await getSyncTaskGroup(detail.groupId);
    setDetail(latest.data);
    message.success(result.data.message);
    actionRef.current?.reload();
  };

  const columns: ProColumns<SyncTaskGroupVO>[] = [
    { title: '任务组', dataIndex: 'groupName', width: 220 },
    { title: '粒度', dataIndex: 'syncScope', width: 100, search: false, render: (_, row) => <Tag color={row.syncScope === 'DATABASE' ? 'processing' : 'default'}>{row.syncScope === 'DATABASE' ? '整库' : '多表'}</Tag> },
    { title: '表数量', dataIndex: 'items', width: 90, search: false, render: (_, row) => row.items?.length || 0 },
    { title: '版本', dataIndex: 'configVersion', width: 80, search: false },
    { title: '状态', dataIndex: 'status', width: 120, valueEnum: groupStatusLabels, render: (_, row) => <Tag color={row.status === 'RUNNING' || row.status === 'VALID' ? 'success' : row.status === 'DEGRADED' ? 'warning' : row.status === 'FAILED' || row.status === 'INVALID' ? 'error' : 'default'}>{groupStatusLabel(row.status)}</Tag> },
    { title: '操作', valueType: 'option', width: 460, fixed: 'right', render: (_, row) => <RowActions actions={[can('sync:group:query') && { key: 'detail', label: '详情', icon: <EyeOutlined />, onClick: () => openDetail(row) }, row.syncScope === 'DATABASE' && can('sync:group:discover') && { key: 'discover', label: '扫描新表', icon: <ReloadOutlined />, onClick: () => discover(row) }, can('sync:group:ddl-check') && { key: 'ddl-check', label: '结构检查', icon: <SafetyCertificateOutlined />, onClick: () => ddlCheck(row) }, can('sync:group:validate') && { key: 'validate', label: '校验', icon: <SafetyCertificateOutlined />, onClick: () => validate(row) }, can('sync:group:engine-config') && { key: 'preview', label: '配置预览', icon: <EyeOutlined />, onClick: () => preview(row) }, can('sync:group:start') && { key: 'start', label: '启动', icon: <CheckCircleOutlined />, disabled: ['RUNNING', 'PAUSING', 'DEGRADED'].includes(row.status || ''), onClick: () => operate(row, 'start') }, can('sync:group:status') && { key: 'status', label: '刷新', icon: <ReloadOutlined />, onClick: () => operate(row, 'status') }, can('sync:group:pause') && { key: 'pause', label: '暂停', icon: <PauseCircleOutlined />, disabled: !['RUNNING', 'DEGRADED'].includes(row.status || ''), onClick: () => operate(row, 'pause') }, can('sync:group:resume') && { key: 'resume', label: '恢复', icon: <PlayCircleOutlined />, disabled: !['PAUSED', 'FAILED'].includes(row.status || ''), onClick: () => operate(row, 'resume') }, can('sync:group:stop') && { key: 'stop', label: '停止', icon: <StopOutlined />, danger: true, disabled: !['RUNNING', 'PAUSING', 'PAUSED', 'FAILED', 'DEGRADED'].includes(row.status || ''), onClick: () => operate(row, 'stop') }, can('sync:group:edit') && { key: 'edit', label: '修改', icon: <EditOutlined />, disabled: ['RUNNING', 'DEGRADED'].includes(row.status || ''), onClick: () => openEdit(row) }, can('sync:group:remove') && { key: 'delete', label: '删除', icon: <DeleteOutlined />, danger: true, disabled: ['RUNNING', 'DEGRADED'].includes(row.status || ''), onClick: () => remove(row) }]} /> }
  ];

  return <PageContainer title="多表同步">
    <ProTable<SyncTaskGroupVO, SyncTaskGroupQuery> actionRef={actionRef} rowKey="groupId" columns={columns} scroll={tableScroll} search={{ labelWidth: 80 }} pagination={{ defaultPageSize: 10 }} request={async params => toTableData(await listSyncTaskGroups(toPageQuery(params)))} toolbar={{ title: '多表同步任务组' }} toolBarRender={() => [can('sync:group:add') && <Button key="add" type="primary" icon={<PlusOutlined />} onClick={openAdd}>新增任务组</Button>]} />
    <ModalForm<SyncTaskGroupForm> title={modalTitle} open={modalOpen} form={form} width={880} layout="vertical" modalProps={{ destroyOnHidden: true, onCancel: () => setModalOpen(false) }} onOpenChange={setModalOpen} onFinish={submit}>
      <ProFormText name="groupId" hidden />
      <ProFormText name="groupName" label="任务组名称" rules={[{ required: true, message: '请输入任务组名称' }]} />
      <ProFormRadio name="syncScope" label="同步粒度" options={[{ label: '多表', value: 'MULTI_TABLE' }, { label: '整库', value: 'DATABASE' }]} />
       <Space style={{ width: '100%' }} align="start"><ProFormSelect name="sourceId" label="源数据源（MySQL）" options={sourceOptions} rules={[{ required: true }]} fieldProps={{ style: { width: 300 }, onChange: value => void loadTables(value as string | number) }} /><ProFormSelect name="targetId" label="目标数据源（PostgreSQL）" options={targetOptions} rules={[{ required: true }]} fieldProps={{ style: { width: 300 } }} /></Space>
      <Divider titlePlacement="left" plain>源库保护（每个表项）</Divider>
      <Space wrap align="start">
        <ProFormDigit name="readLimitRowsPerSecond" label="最大行数/秒" min={1} max={100000} rules={[{ required: true }]} fieldProps={{ style: { width: 170 }}} />
        <ProFormDigit name="readLimitBytesPerSecond" label="最大字节/秒" min={1} max={1073741824} rules={[{ required: true }]} fieldProps={{ style: { width: 190 }}} />
        <ProFormDigit name="snapshotParallelism" label="快照并行度" min={1} max={4} rules={[{ required: true }]} fieldProps={{ style: { width: 150 }}} />
        <ProFormDigit name="sourceConnectionLimit" label="CDC 连接池上限" min={1} max={8} rules={[{ required: true }]} fieldProps={{ style: { width: 170 }}} />
      </Space>
      <Alert type="info" showIcon message="任务组按表提交独立作业，以上限速与连接数分别作用于每个表项。" />
      {syncScope === 'DATABASE' ? <Space style={{ width: '100%' }} align="start"><ProFormText name="sourceDatabase" label="源数据库" placeholder="默认使用数据源数据库" fieldProps={{ style: { width: 300 } }} /><ProFormSwitch name="autoDiscover" label="自动发现新表" fieldProps={{ checkedChildren: '开启', unCheckedChildren: '关闭' }} convertValue={value => value === '1'} transform={value => ({ autoDiscover: value ? '1' : '0' })} /></Space> : <Form.List name="items">{(fields, { add, remove }) => <Space direction="vertical" style={{ width: '100%' }} size={8}>{fields.map(field => { const metadata = itemMetadata[String(field.name)]; return <div key={field.key} style={{ borderBottom: '1px solid #f0f0f0', paddingBottom: 12 }}><Space align="start" wrap><Form.Item {...field} name={[field.name, 'sourceDatabase']} hidden><Input /></Form.Item><Form.Item {...field} name={[field.name, 'sourceTable']} label="源表" rules={[{ required: true, message: '请选择源表' }]}><Select showSearch options={sourceTables.map(table => ({ label: table, value: table }))} style={{ width: 220 }} onChange={value => void loadItemMetadata(field.name, value)} /></Form.Item><Form.Item {...field} name={[field.name, 'targetSchema']} label="目标Schema"><Input placeholder="public" style={{ width: 120 }} /></Form.Item><Form.Item {...field} name={[field.name, 'targetTable']} label="目标表" rules={[{ required: true, message: '请输入目标表' }]}><Input style={{ width: 220 }} /></Form.Item><Button danger type="text" onClick={() => remove(field.name)}>删除</Button></Space>{metadata && <Space direction="vertical" size={4} style={{ width: '100%' }}><Form.Item {...field} name={[field.name, 'selectedColumns']} label="同步字段" rules={[{ required: true, message: '至少选择一个同步字段' }]} extra="同步键字段不可排除。"><Checkbox.Group options={metadata.columns.map(column => ({ label: `${column.name} (${column.typeName || '-'})`, value: column.name }))} /></Form.Item><Form.Item {...field} name={[field.name, 'syncKeyColumns']} label="同步键" rules={[{ required: true, message: '请选择可靠同步键' }]}><Select options={keyOptions(metadata)} disabled={keyOptions(metadata).length === 0} placeholder="请选择主键或非空唯一键" /></Form.Item>{keyOptions(metadata).length === 0 && <Alert type="warning" showIcon message="该表没有可靠同步键，无法加入 CDC 任务组。" />}</Space>}</div>; })}<Button type="dashed" onClick={() => add({ targetSchema: 'public' })}>添加表</Button></Space>}</Form.List>}
    </ModalForm>
    <Modal title={detail ? `任务组详情：${detail.groupName}` : '任务组详情'} open={detailOpen} width={1040} footer={null} destroyOnHidden onCancel={() => setDetailOpen(false)}>
      {detail && <Space direction="vertical" size={16} style={{ width: '100%' }}>
        <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }}>
          <Descriptions.Item label="任务组状态">{groupStatusLabel(detail.status)}</Descriptions.Item>
          <Descriptions.Item label="同步粒度">{detail.syncScope === 'DATABASE' ? '整库' : '多表'}</Descriptions.Item>
          <Descriptions.Item label="表数量">{detail.items.length}</Descriptions.Item>
          <Descriptions.Item label="同步模式">{detail.syncMode === 'FULL_CDC' ? '全量 + CDC' : detail.syncMode || '-'}</Descriptions.Item>
          <Descriptions.Item label="配置版本">{detail.configVersion || '-'}</Descriptions.Item>
          <Descriptions.Item label="最近检查点">{detail.lastCheckpointTime || '-'}</Descriptions.Item>
          <Descriptions.Item label="每表最大行数/秒">{detail.readLimitRowsPerSecond ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="每表最大字节/秒">{detail.readLimitBytesPerSecond ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="每表快照并行度">{detail.snapshotParallelism ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="每表 CDC 连接池">{detail.sourceConnectionLimit ?? '-'}</Descriptions.Item>
        </Descriptions>
        <Divider titlePlacement="left" plain>数据质量核对</Divider>
        {can('sync:group:check') && <Button icon={<CheckCircleOutlined />} onClick={checkData}>执行逐表核对</Button>}
        {dataCheckResult && <Alert type={dataCheckResult.matched ? 'success' : 'warning'} showIcon message={dataCheckResult.message} description={<Descriptions size="small" column={{ xs: 1, sm: 2, md: 4 }}>
          <Descriptions.Item label="一致表">{dataCheckResult.matchedTableCount}</Descriptions.Item>
          <Descriptions.Item label="不一致表">{dataCheckResult.mismatchedTableCount}</Descriptions.Item>
          <Descriptions.Item label="执行失败">{dataCheckResult.failedTableCount}</Descriptions.Item>
          <Descriptions.Item label="核对范围">{dataCheckResult.tableCount} 张表</Descriptions.Item>
          <Descriptions.Item label="说明" span={4}>{dataCheckResult.consistencyNote}</Descriptions.Item>
        </Descriptions>} />}
        <Descriptions bordered size="small" column={1} title="逐表最近核对结果">
          {detail.items.map(item => <Descriptions.Item key={String(item.itemId)} label={`${item.sourceDatabase || '-'} . ${item.sourceTable} -> ${item.targetSchema || 'public'} . ${item.targetTable}`}>
            {item.lastCheckTime ? <Space wrap size={8}><Tag color={item.lastCheckMatched === '1' ? 'success' : 'error'}>{item.lastCheckMatched === '1' ? '行数一致' : '未一致 / 失败'}</Tag><span>源 {item.lastCheckSourceRows ?? '-'}，目标 {item.lastCheckTargetRows ?? '-'}，差异 {item.lastCheckDifference ?? '-'}</span><span>{item.lastCheckTime}</span><span>{item.lastCheckMessage}</span></Space> : <Tag>尚未核对</Tag>}
          </Descriptions.Item>)}
        </Descriptions>
      </Space>}
    </Modal>
  </PageContainer>;
}
