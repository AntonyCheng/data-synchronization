import { CheckCircleOutlined, DeleteOutlined, EditOutlined, EyeOutlined, PauseCircleOutlined, PlayCircleOutlined, PlusOutlined, ReloadOutlined, SafetyCertificateOutlined, StopOutlined } from '@ant-design/icons';
import { ModalForm, PageContainer, ProFormDependency, ProFormDigit, ProFormSelect, ProFormSwitch, ProFormText, ProTable, type ActionType, type ProColumns } from '@ant-design/pro-components';
import { Alert, AutoComplete, Button, Checkbox, Descriptions, Divider, Form, Input, message, Modal, Radio, Select, Space, Tag } from 'antd';
import { useEffect, useRef, useState } from 'react';
import { createKafkaTopic, getDataSourceMetadata, listDataSourceTables, listDataSources, listKafkaTopics } from '@/api/sync/data-source';
import type { DataSourceMetadataVO, DataSourceVO } from '@/api/sync/data-source/types';
import { addSyncTaskGroup, checkSyncTaskGroupData, checkSyncTaskGroupDdl, deleteSyncTaskGroup, discoverSyncTaskGroupTables, getSyncTaskGroup, listSyncTaskGroups, pauseSyncTaskGroup, previewSyncTaskGroupConfig, refreshSyncTaskGroupStatus, resumeSyncTaskGroup, resumeSyncTaskGroupItemAfterDdl, startSyncTaskGroup, stopSyncTaskGroup, updateSyncTaskGroup, validateSyncTaskGroup } from '@/api/sync/group';
import type { SyncTaskGroupDataCheckResult, SyncTaskGroupForm, SyncTaskGroupQuery, SyncTaskGroupVO } from '@/api/sync/group/types';
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
  const [targetTables, setTargetTables] = useState<string[]>([]);
  const [targetTablesLoading, setTargetTablesLoading] = useState(false);
  const [topicCreateLoading, setTopicCreateLoading] = useState<number>();
  const [itemMetadata, setItemMetadata] = useState<Record<string, DataSourceMetadataVO>>({});
  const [modalOpen, setModalOpen] = useState(false);
  const [modalTitle, setModalTitle] = useState('');
  const [detail, setDetail] = useState<SyncTaskGroupVO>();
  const [detailOpen, setDetailOpen] = useState(false);
  const [dataCheckResult, setDataCheckResult] = useState<SyncTaskGroupDataCheckResult>();
  const [validationResult, setValidationResult] = useState<Awaited<ReturnType<typeof validateSyncTaskGroup>>['data']>();
  const [configPreview, setConfigPreview] = useState<Awaited<ReturnType<typeof previewSyncTaskGroupConfig>>['data']>();
  const [ddlResult, setDdlResult] = useState<Awaited<ReturnType<typeof checkSyncTaskGroupDdl>>['data']>();
  const syncScope = Form.useWatch('syncScope', form) || 'MULTI_TABLE';
  const syncMode = Form.useWatch('syncMode', form) || 'FULL_CDC';
  const targetId = Form.useWatch('targetId', form);
  const selectedTarget = dataSources.find(item => String(item.sourceId) === String(targetId));
  const kafkaTarget = selectedTarget?.sourceType === 'KAFKA';
  const postgresTarget = selectedTarget?.sourceType === 'POSTGRESQL';
  const detailTarget = dataSources.find(item => String(item.sourceId) === String(detail?.targetId));

  const can = (permission: string) => hasPermi(userInfo, [permission]);
  const sourceOptions = dataSources.filter(item => item.sourceType === 'MYSQL').map(item => ({ label: `${item.sourceName} (${item.databaseName})`, value: item.sourceId }));
  const targetOptions = dataSources
    .filter(item => item.sourceType === 'POSTGRESQL' || item.sourceType === 'MYSQL' || item.sourceType === 'KAFKA')
    .map(item => ({ label: `${item.sourceName} (${item.databaseName})`, value: item.sourceId }));

  useEffect(() => { listDataSources({ pageNum: 1, pageSize: 100 }).then(res => setDataSources(res.data?.rows || [])); }, []);


  const loadTables = async (sourceId?: string | number) => {
    const source = dataSources.find(item => String(item.sourceId) === String(sourceId));
    if (source) setSourceTables((await listDataSourceTables(source.sourceId, source.databaseName)).data || []);
  };

  const loadTargetTables = async (targetSourceId?: string | number) => {
    const target = dataSources.find(item => String(item.sourceId) === String(targetSourceId));
    if (!target) return;
    setTargetTablesLoading(true);
    try {
      if (target.sourceType === 'KAFKA') {
        const result = await listKafkaTopics(target.sourceId);
        setTargetTables((result.data || []).map(item => item.topic));
      } else {
        const result = await listDataSourceTables(target.sourceId, target.databaseName);
        setTargetTables(result.data || []);
      }
    } finally {
      setTargetTablesLoading(false);
    }
  };

  const createGroupTopic = async (itemIndex: number) => {
    if (!targetId || !kafkaTarget) return;
    const topic = String(form.getFieldValue(['items', itemIndex, 'targetTable']) || '').trim();
    if (!topic) {
      message.warning('请先填写要创建的 topic 名称');
      return;
    }
    setTopicCreateLoading(itemIndex);
    try {
      const result = await createKafkaTopic(targetId, { topic });
      form.setFieldValue(['items', itemIndex, 'targetTable'], result.data.topic);
      await loadTargetTables(targetId);
      message.success(`topic ${result.data.topic} 创建成功`);
    } finally {
      setTopicCreateLoading(undefined);
    }
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

  const openAdd = () => { form.resetFields(); form.setFieldsValue(emptyForm); setItemMetadata({}); setTargetTables([]); setModalTitle('新增多表同步任务'); setModalOpen(true); };
  const openEdit = async (row: SyncTaskGroupVO) => { const result = await getSyncTaskGroup(row.groupId); form.resetFields(); form.setFieldsValue(result.data); setItemMetadata({}); await loadTables(result.data.sourceId); await loadTargetTables(result.data.targetId); await Promise.all((result.data.items || []).map((item, index) => loadItemMetadata(index, item.sourceTable, result.data.sourceId))); setModalTitle('修改多表同步任务'); setModalOpen(true); };
  const openDetail = async (row: SyncTaskGroupVO) => { const result = await getSyncTaskGroup(row.groupId); setDetail(result.data); setDataCheckResult(undefined); setValidationResult(undefined); setConfigPreview(undefined); setDdlResult(undefined); setDetailOpen(true); };
  const submit = async (values: SyncTaskGroupForm) => {
    const databaseScope = values.syncScope === 'DATABASE';
    const payload = {
      ...values,
      // Form.List values remain mounted when users switch to whole-database mode.
      // Whole-database groups discover their items server-side and must not submit them.
      items: databaseScope ? [] : values.items?.map(item => ({ ...item, selectedColumns: Array.isArray(item.selectedColumns) ? item.selectedColumns.join(',') : item.selectedColumns }))
    };
    if (values.groupId) await updateSyncTaskGroup(payload); else await addSyncTaskGroup(payload);
    message.success('保存成功');
    setModalOpen(false);
    actionRef.current?.reload();
    return true;
  };
  const remove = async (row: SyncTaskGroupVO) => { await deleteSyncTaskGroup(row.groupId); message.success('删除成功'); setDetailOpen(false); actionRef.current?.reloadAndRest?.(); };
  // Validation is a read-only check and never changes the group's actual lifecycle
  // status - overwriting detail.status with a synthetic 'VALID'/'INVALID' here (as this
  // used to) desynced the displayed status and run-control buttons from backend truth
  // (e.g. a RUNNING group would appear stopped, enabling 启动 and blocking 中止/删除
  // for the wrong reason). Keep the validation outcome only in validationResult.
  const validate = async (row: SyncTaskGroupVO) => { const result = await validateSyncTaskGroup(row.groupId); setValidationResult(result.data); };
  const preview = async (row: SyncTaskGroupVO) => { const result = await previewSyncTaskGroupConfig(row.groupId); setConfigPreview(result.data); };
  const operate = async (row: SyncTaskGroupVO, action: 'start' | 'status' | 'pause' | 'resume' | 'stop') => {
    const request = { start: startSyncTaskGroup, status: refreshSyncTaskGroupStatus, pause: pauseSyncTaskGroup, resume: resumeSyncTaskGroup, stop: stopSyncTaskGroup }[action];
    const result = await request(row.groupId);
    message.success(result.data.message);
    const latest = await getSyncTaskGroup(row.groupId);
    setDetail(current => current ? latest.data : current);
    actionRef.current?.reload();
  };
  const discover = async (row: SyncTaskGroupVO) => { const result = await discoverSyncTaskGroupTables(row.groupId); message.success(result.data.message); actionRef.current?.reload(); };
  const ddlCheck = async (row: SyncTaskGroupVO) => {
    const result = await checkSyncTaskGroupDdl(row.groupId);
    setDdlResult(result.data);
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
    { title: '操作', valueType: 'option', width: 170, fixed: 'right', render: (_, row) => <Space size={6} wrap={false} className="group-row-actions-react">
      {can('sync:group:query') && <Button type="link" size="small" icon={<EyeOutlined />} onClick={() => openDetail(row)}>详情</Button>}
      {can('sync:group:remove') && <Button type="link" size="small" danger icon={<DeleteOutlined />} disabled={['RUNNING', 'PAUSING', 'DEGRADED'].includes(row.status || '')} title={['RUNNING', 'PAUSING', 'DEGRADED'].includes(row.status || '') ? '运行中的任务组不能删除' : undefined} onClick={() => Modal.confirm({ title: '删除同步任务组', content: `是否确认删除任务组“${row.groupName}”？`, okText: '确认删除', cancelText: '取消', onOk: () => remove(row) })}>删除</Button>}
    </Space> }
  ];

  return <PageContainer title="多表同步">
    <ProTable<SyncTaskGroupVO, SyncTaskGroupQuery> actionRef={actionRef} rowKey="groupId" columns={columns} scroll={tableScroll} search={{ labelWidth: 80 }} pagination={{ defaultPageSize: 10 }} request={async params => toTableData(await listSyncTaskGroups(toPageQuery(params)))} toolbar={{ title: '多表同步任务组' }} toolBarRender={() => [can('sync:group:add') && <Button key="add" type="primary" icon={<PlusOutlined />} onClick={openAdd}>新增任务组</Button>]} />
    <ModalForm<SyncTaskGroupForm> title={modalTitle} open={modalOpen} form={form} width={880} layout="vertical" modalProps={{ destroyOnHidden: true, onCancel: () => setModalOpen(false) }} onOpenChange={setModalOpen} onFinish={submit}>
      <ProFormText name="groupId" hidden />
      <ProFormText name="groupName" label="任务组名称" rules={[{ required: true, message: '请输入任务组名称' }]} />
      <Form.Item
        name="syncScope"
        label="同步粒度"
        rules={[{ required: true, message: '请选择同步粒度' }]}
        extra="多表用于手动选择表；整库会扫描当前数据库，并可持续发现新表。"
      >
        <Radio.Group aria-label="同步粒度">
          <Radio value="MULTI_TABLE">多表同步</Radio>
          <Radio value="DATABASE">整库同步</Radio>
        </Radio.Group>
      </Form.Item>
      <Form.Item name="syncMode" label="同步方式" rules={[{ required: true, message: '请选择同步方式' }]}>
        <Radio.Group aria-label="同步方式">
          <Radio value="FULL">全量</Radio>
          <Radio value="INCREMENTAL">增量</Radio>
          <Radio value="FULL_CDC">全量 + CDC</Radio>
        </Radio.Group>
      </Form.Item>
       <Space style={{ width: '100%' }} align="start"><ProFormSelect name="sourceId" label="源数据源（MySQL）" options={sourceOptions} rules={[{ required: true }]} fieldProps={{ style: { width: 300 }, onChange: value => void loadTables(value as string | number) }} /><ProFormDependency name={['sourceId']}>{({ sourceId }) => <ProFormSelect name="targetId" label="目标数据源（MySQL / PostgreSQL / Kafka）" options={targetOptions.filter(option => String(option.value) !== String(sourceId))} rules={[{ required: true }]} fieldProps={{ style: { width: 300 }, onChange: value => { const target = dataSources.find(item => String(item.sourceId) === String(value)); const items = form.getFieldValue('items') || []; form.setFieldsValue({ items: items.map((item: NonNullable<SyncTaskGroupForm['items']>[number]) => ({ ...item, targetSchema: target?.sourceType === 'POSTGRESQL' ? (item.targetSchema || 'public') : undefined })) }); setTargetTables([]); void loadTargetTables(value as string | number); } }} />}</ProFormDependency></Space>
      <Divider titlePlacement="left" plain>源库保护（每个表项）</Divider>
      <Space wrap align="start">
        <ProFormDigit name="readLimitRowsPerSecond" label="最大行数/秒" min={1} max={100000} rules={[{ required: true }]} fieldProps={{ style: { width: 170 }}} />
        <ProFormDigit name="readLimitBytesPerSecond" label="最大字节/秒" min={1} max={1073741824} rules={[{ required: true }]} fieldProps={{ style: { width: 190 }}} />
        <ProFormDigit name="snapshotParallelism" label="快照并行度" min={1} max={4} rules={[{ required: true }]} fieldProps={{ style: { width: 150 }}} />
        <ProFormDigit name="sourceConnectionLimit" label="CDC 连接池上限" min={1} max={8} rules={[{ required: true }]} fieldProps={{ style: { width: 170 }}} />
      </Space>
      <Alert type="info" showIcon message="任务组按表提交独立作业，以上限速与连接数分别作用于每个表项。" />
      {syncScope === 'DATABASE' ? <Space style={{ width: '100%' }} align="start"><ProFormText name="sourceDatabase" label="源数据库" placeholder="默认使用数据源数据库" fieldProps={{ style: { width: 300 } }} /><ProFormSwitch name="autoDiscover" label="自动发现新表" fieldProps={{ checkedChildren: '开启', unCheckedChildren: '关闭' }} convertValue={value => value === '1'} transform={value => ({ autoDiscover: value ? '1' : '0' })} /></Space> : <Form.List name="items">{(fields, { add, remove }) => <Space direction="vertical" style={{ width: '100%' }} size={8}>{fields.map(field => { const metadata = itemMetadata[String(field.name)]; return <div key={field.key} style={{ borderBottom: '1px solid #f0f0f0', paddingBottom: 12 }}><Space align="start" wrap><Form.Item {...field} name={[field.name, 'sourceDatabase']} hidden><Input /></Form.Item><Form.Item {...field} name={[field.name, 'sourceTable']} label="源表" rules={[{ required: true, message: '请选择源表' }, { validator: (_rule, value) => { if (!value) return Promise.resolve(); const items: NonNullable<SyncTaskGroupForm['items']> = form.getFieldValue('items') || []; const duplicate = items.some((item, index) => index !== field.name && item?.sourceTable === value); return duplicate ? Promise.reject(new Error('该表已在本任务组中选过')) : Promise.resolve(); } }]}><Select showSearch options={sourceTables.map(table => ({ label: table, value: table }))} style={{ width: 220 }} onChange={value => void loadItemMetadata(field.name, value)} /></Form.Item>{postgresTarget && <Form.Item {...field} name={[field.name, 'targetSchema']} label="目标 Schema"><Input placeholder="public" style={{ width: 120 }} /></Form.Item>}<Form.Item {...field} name={[field.name, 'targetTable']} label={kafkaTarget ? '目标 topic' : '目标表'} rules={[{ required: true, message: kafkaTarget ? '请选择或填写目标 topic' : '请输入目标表' }]}>{kafkaTarget ? <AutoComplete options={targetTables.map(table => ({ label: table, value: table }))} allowClear placeholder={targetTablesLoading ? '正在读取 topic 列表' : '选择已有 topic 或输入新 topic'} style={{ width: 220 }} /> : <AutoComplete options={targetTables.map(table => ({ label: table, value: table }))} allowClear placeholder={targetTablesLoading ? '正在读取目标表，可直接输入新表名' : '选择已有表或输入新表名'} style={{ width: 220 }} />}</Form.Item>{kafkaTarget && <Button type="default" loading={topicCreateLoading === field.name} onClick={() => void createGroupTopic(field.name)}>创建 topic</Button>}<Button danger type="text" onClick={() => remove(field.name)}>删除</Button></Space>{kafkaTarget && <Alert type="info" showIcon message="可选择已有 topic，也可输入名称后点击“创建 topic”；平台不会依赖 broker 自动建 topic。" style={{ marginTop: 8 }} />}{!kafkaTarget && <Alert type="info" showIcon message="可选择目标库已有表，也可直接输入新表名；新表启动时按源表字段结构自动创建。" style={{ marginTop: 8 }} />}{metadata && <Space direction="vertical" size={4} style={{ width: '100%' }}><Form.Item {...field} name={[field.name, 'selectedColumns']} label="同步字段" rules={[{ required: true, message: '至少选择一个同步字段' }]} extra="同步键字段不可排除。"><Checkbox.Group options={metadata.columns.map(column => ({ label: `${column.name} (${column.typeName || '-'})`, value: column.name }))} /></Form.Item><Form.Item {...field} name={[field.name, 'syncKeyColumns']} label="同步键" rules={[{ required: true, message: '请选择可靠同步键' }]}><Select options={keyOptions(metadata)} disabled={keyOptions(metadata).length === 0} placeholder="请选择主键或非空唯一键" /></Form.Item>{keyOptions(metadata).length === 0 && <Alert type="warning" showIcon message="该表没有可靠同步键，无法加入 CDC 任务组。" />}</Space>}</div>; })}<Button type="dashed" onClick={() => add({ targetSchema: postgresTarget ? 'public' : undefined })}>添加表</Button></Space>}</Form.List>}
    </ModalForm>
    <Modal title={detail ? `任务组详情：${detail.groupName}` : '任务组详情'} open={detailOpen} width={1040} footer={null} destroyOnHidden onCancel={() => setDetailOpen(false)}>
      {detail && <Space direction="vertical" size={16} style={{ width: '100%' }}>
        <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }}>
          <Descriptions.Item label="任务组状态">{groupStatusLabel(detail.status)}</Descriptions.Item>
          <Descriptions.Item label="同步粒度">{detail.syncScope === 'DATABASE' ? '整库' : '多表'}</Descriptions.Item>
          <Descriptions.Item label="表数量">{detail.items.length}</Descriptions.Item>
          <Descriptions.Item label="同步模式">{{ FULL: '全量', INCREMENTAL: '增量', FULL_CDC: '全量 + CDC' }[detail.syncMode || ''] || detail.syncMode || '-'}</Descriptions.Item>
          <Descriptions.Item label="配置版本">{detail.configVersion || '-'}</Descriptions.Item>
          <Descriptions.Item label="最近检查点">{detail.lastCheckpointTime || '-'}</Descriptions.Item>
          <Descriptions.Item label="每表最大行数/秒">{detail.readLimitRowsPerSecond ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="每表最大字节/秒">{detail.readLimitBytesPerSecond ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="每表快照并行度">{detail.snapshotParallelism ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="每表 CDC 连接池">{detail.sourceConnectionLimit ?? '-'}</Descriptions.Item>
        </Descriptions>
        <Divider titlePlacement="left" plain>运行控制</Divider>
        <Space wrap size={8}>
          {can('sync:group:start') && <Button type="primary" icon={<PlayCircleOutlined />} disabled={['RUNNING', 'PAUSING', 'DEGRADED'].includes(detail.status || '')} onClick={() => operate(detail, 'start')}>启动</Button>}
          {can('sync:group:status') && <Button icon={<ReloadOutlined />} onClick={() => operate(detail, 'status')}>刷新状态</Button>}
          {can('sync:group:pause') && <Button icon={<PauseCircleOutlined />} disabled={!['RUNNING', 'DEGRADED'].includes(detail.status || '')} onClick={() => operate(detail, 'pause')}>暂停</Button>}
          {can('sync:group:resume') && <Button icon={<PlayCircleOutlined />} disabled={!['PAUSED', 'FAILED'].includes(detail.status || '')} onClick={() => operate(detail, 'resume')}>恢复</Button>}
          {can('sync:group:stop') && <Button danger icon={<StopOutlined />} disabled={!['RUNNING', 'PAUSING', 'PAUSED', 'FAILED', 'DEGRADED'].includes(detail.status || '')} onClick={() => Modal.confirm({ title: '中止同步任务组', content: '中止后不会保留可恢复状态，是否继续？', okText: '确认中止', cancelText: '取消', onOk: () => operate(detail, 'stop') })}>中止</Button>}
        </Space>
        <Divider titlePlacement="left" plain>配置与发现</Divider>
        <Space wrap size={8}>
          {can('sync:group:edit') && <Button icon={<EditOutlined />} disabled={['RUNNING', 'PAUSING', 'DEGRADED'].includes(detail.status || '')} onClick={() => { setDetailOpen(false); openEdit(detail); }}>修改任务组</Button>}
          {can('sync:group:discover') && detail.syncScope === 'DATABASE' && <Button icon={<ReloadOutlined />} onClick={() => discover(detail)}>扫描新表</Button>}
          {can('sync:group:engine-config') && <Button icon={<EyeOutlined />} onClick={() => preview(detail)}>配置预览</Button>}
          {can('sync:group:validate') && <Button icon={<SafetyCertificateOutlined />} onClick={() => validate(detail)}>连接与配置校验</Button>}
          {can('sync:group:ddl-check') && <Button icon={<SafetyCertificateOutlined />} onClick={() => ddlCheck(detail)}>结构检查</Button>}
        </Space>
        {validationResult && <Alert type={validationResult.valid ? 'success' : 'error'} showIcon closable onClose={() => setValidationResult(undefined)} message={validationResult.message} description={<Descriptions size="small" column={1}><Descriptions.Item label="源连接">{validationResult.source.message}</Descriptions.Item><Descriptions.Item label="目标连接">{validationResult.target.message}</Descriptions.Item>{validationResult.items.map(item => <Descriptions.Item key={String(item.itemId)} label={`${item.sourceTable} → ${item.targetTable}`}><Tag color={item.passed ? 'success' : 'error'}>{item.passed ? '通过' : '失败'}</Tag> {item.message}</Descriptions.Item>)}</Descriptions>} />}
        {configPreview && <Alert type="success" showIcon closable onClose={() => setConfigPreview(undefined)} message={`${configPreview.groupName} 配置预览`} description={<Input.TextArea value={configPreview.config} readOnly autoSize={{ minRows: 12, maxRows: 24 }} style={{ width: '100%', resize: 'vertical' }} />} />}
        {ddlResult && <Alert type={ddlResult.events.length ? 'warning' : 'success'} showIcon closable onClose={() => setDdlResult(undefined)} message={ddlResult.message} description={ddlResult.events.length === 0 ? undefined : <Space direction="vertical" style={{ width: '100%' }} size={12}>{ddlResult.events.map(event => <Descriptions key={String(event.eventId)} size="small" bordered column={1} title={<Space><span>{event.sourceTable} → {event.targetTable}</span><Tag color={event.riskLevel === 'HIGH' ? 'error' : 'warning'}>{event.riskLevel === 'HIGH' ? '高风险' : '低风险'}</Tag><Tag color={event.status === 'READY_TO_RESUME' ? 'success' : 'error'}>{event.status === 'READY_TO_RESUME' ? '可恢复' : '待修复'}</Tag></Space>}><Descriptions.Item label="变更类型">{event.changeType}</Descriptions.Item><Descriptions.Item label="变更详情">{event.details}</Descriptions.Item><Descriptions.Item label="处理建议">{event.remediation}</Descriptions.Item><Descriptions.Item label="操作">{event.status === 'READY_TO_RESUME' ? <Button type="primary" size="small" onClick={async () => { await resumeSyncTaskGroupItemAfterDdl(detail.groupId, event.itemId); message.success('表项已恢复'); await ddlCheck(detail); }}>恢复该表</Button> : <span>修复目标表后重新执行检查</span>}</Descriptions.Item></Descriptions>)}</Space>} />}
        <Divider titlePlacement="left" plain>危险操作</Divider>
        <Button danger icon={<DeleteOutlined />} disabled={['RUNNING', 'PAUSING', 'DEGRADED'].includes(detail.status || '')} title={['RUNNING', 'PAUSING', 'DEGRADED'].includes(detail.status || '') ? '运行中的任务组不能删除' : undefined} onClick={() => Modal.confirm({ title: '删除同步任务组', content: `是否确认删除任务组“${detail.groupName}”？`, okText: '确认删除', cancelText: '取消', onOk: () => remove(detail) })}>删除任务组</Button>
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
          {detail.items.map(item => <Descriptions.Item key={String(item.itemId)} label={`${item.sourceDatabase || '-'} . ${item.sourceTable} -> ${detailTarget?.sourceType === 'POSTGRESQL' ? `${item.targetSchema || 'public'} . ` : ''}${item.targetTable}`}>
            {item.lastCheckTime ? <Space wrap size={8}><Tag color={item.lastCheckMatched === '1' ? 'success' : 'error'}>{item.lastCheckMatched === '1' ? '行数一致' : '未一致 / 失败'}</Tag><span>源 {item.lastCheckSourceRows ?? '-'}，目标 {item.lastCheckTargetRows ?? '-'}，差异 {item.lastCheckDifference ?? '-'}</span><span>{item.lastCheckTime}</span><span>{item.lastCheckMessage}</span></Space> : <Tag>尚未核对</Tag>}
          </Descriptions.Item>)}
        </Descriptions>
      </Space>}
    </Modal>
  </PageContainer>;
}
