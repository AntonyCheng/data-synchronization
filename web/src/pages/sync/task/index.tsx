import {
  CheckCircleOutlined,
  CloseOutlined,
  CodeOutlined,
  DeleteOutlined,
  EditOutlined,
  EyeOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
  StopOutlined
} from '@ant-design/icons';
import {
  ModalForm,
  PageContainer,
  ProFormDigit,
  ProFormSelect,
  ProFormText,
  ProTable,
  type ActionType,
  type ProColumns
} from '@ant-design/pro-components';
import { useBoolean } from 'ahooks';
import { Alert, AutoComplete, Button, Checkbox, Descriptions, Divider, Form, InputNumber, message, Modal, Radio, Select, Space, Steps, Table, Tag, Tooltip, Typography } from 'antd';
import { useEffect, useRef, useState } from 'react';
import {
  checkDataSourceCdc,
  getDataSourceMetadata,
  listDataSourceDatabases,
  listDataSourceTables,
  listKafkaTopics,
  createKafkaTopic,
  listDataSources,
  testDataSource
} from '@/api/sync/data-source';
import type { ConnectionTestResult, DataSourceCdcPrecheckVO, DataSourceMetadataVO, DataSourceVO } from '@/api/sync/data-source/types';
import {
  addSyncTask,
  checkTargetCompatibility,
  checkSyncTaskData,
  deleteSyncTask,
  getSyncTask,
  listSyncTasks,
  pauseSyncTask,
  previewSyncTaskConfig,
  refreshSyncTaskStatus,
  reinitializeSyncTask,
  resumeSyncTask,
  startSyncTask,
  stopSyncTask,
  updateSyncTask,
  validateSyncTask
} from '@/api/sync/task';
import type { SeaTunnelJobStatus, SyncTaskForm, SyncTaskQuery, SyncTaskVO, TaskValidationResult } from '@/api/sync/task/types';
import { useTableScroll } from '@/hooks/useTableScroll';
import { useUserStore } from '@/stores/userStore';
import { hasPermi } from '@/utils/permission';
import { toPageQuery, toTableData } from '@/utils/ruoyi';

const defaultForm: SyncTaskForm = { syncMode: 'FULL_CDC', incrementalStartupMode: 'LATEST', fullDataMode: 'UPSERT', ddlPolicy: 'FAIL', scheduleMode: 'ONCE', targetSchema: 'public', readLimitRowsPerSecond: 1000, readLimitBytesPerSecond: 10485760, snapshotParallelism: 1, sourceConnectionLimit: 2 };
const statusLabels: Record<string, string> = {
  DRAFT: '草稿', RUNNING: '运行中', PAUSING: '暂停中', PAUSED: '已暂停', STOPPED: '已停止', FAILED: '失败', FINISHED: '已完成', REINITIALIZE_REQUIRED: '需重新初始化'
};

function sourceLabel(source: DataSourceVO) {
  return `${source.sourceName} (${source.sourceType})`;
}

function statusTag(status?: string, error?: string) {
  const tag = <Tag color={status === 'RUNNING' ? 'processing' : status === 'FAILED' || status === 'REINITIALIZE_REQUIRED' ? 'error' : status === 'PAUSED' ? 'warning' : 'default'}>{statusLabels[status || ''] || status || '-'}</Tag>;
  return error ? <Tooltip title={error}>{tag}</Tooltip> : tag;
}

function diagnosticMessage(title: string, onClose: () => void) {
  return <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', gap: 12 }}>
    <Typography.Text strong>{title}</Typography.Text>
    <Button type="text" size="small" icon={<CloseOutlined />} aria-label="关闭检查结果" title="关闭检查结果" onClick={onClose} />
  </div>;
}

function reliableKeyOptions(metadata?: DataSourceMetadataVO) {
  if (!metadata) return [];
  const options = metadata.primaryKeys.length ? [{ label: `主键（${metadata.primaryKeys.join(', ')}）`, value: metadata.primaryKeys.join(',') }] : [];
  metadata.uniqueKeys.filter(key => key.allNotNull).forEach(key => options.push({ label: `唯一键 ${key.name}（${key.columns.join(', ')}）`, value: key.columns.join(',') }));
  return options;
}

function defaultKafkaTopic(database: string, table: string) {
  return `${database || 'source'}_${table}`.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 249);
}

function mappingRisks(metadata?: DataSourceMetadataVO) {
  if (!metadata) return [];
  const risks: string[] = [];
  const ambiguous = metadata.columns.filter(column => /tinyint/i.test(column.typeName || '') && (column.size || 0) <= 1).map(column => column.name);
  const large = metadata.columns.filter(column => /(text|blob|json)/i.test(column.typeName || '')).map(column => column.name);
  if (ambiguous.length) risks.push(`字段 ${ambiguous.join(', ')} 可能是布尔语义 tinyint(1)，请确认目标端类型。`);
  if (large.length) risks.push(`字段 ${large.join(', ')} 为 TEXT/BLOB/JSON，可能显著增加读取字节量。`);
  if (metadata.charset && !/utf8|unicode/i.test(metadata.charset)) risks.push(`源表字符集为 ${metadata.charset}，请关注旧编码的转换风险。`);
  return risks;
}

export default function SyncTaskPage() {
  const actionRef = useRef<ActionType | undefined>(undefined);
  const pollingTokenRef = useRef(0);
  const { tableScroll } = useTableScroll(1080);
  const [form] = Form.useForm<SyncTaskForm>();
  const userInfo = useUserStore(state => state.userInfo);
  const [modalOpen, { setTrue: openModal, setFalse: closeModal }] = useBoolean(false);
  const [modalTitle, setModalTitle] = useState('');
  const [wizardStep, setWizardStep] = useState(0);
  const [dataSources, setDataSources] = useState<DataSourceVO[]>([]);
  const [sourceDatabases, setSourceDatabases] = useState<string[]>([]);
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [sourceDatabase, setSourceDatabase] = useState('');
  const [targetTables, setTargetTables] = useState<string[]>([]);
  const [targetTablesLoading, setTargetTablesLoading] = useState(false);
  const [topicMode, setTopicMode] = useState<'EXISTING' | 'CREATE'>('EXISTING');
  const [topicCreateLoading, setTopicCreateLoading] = useState(false);
  const [sourceMetadata, setSourceMetadata] = useState<DataSourceMetadataVO>();
  const [cdcPrecheck, setCdcPrecheck] = useState<DataSourceCdcPrecheckVO | TaskValidationResult['cdcPrecheck']>();
  const [sourceConnectionTest, setSourceConnectionTest] = useState<ConnectionTestResult>();
  const [metadataLoading, setMetadataLoading] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [selectedTask, setSelectedTask] = useState<SyncTaskVO>();
  const [detailLoading, setDetailLoading] = useState(false);
  const [busyAction, setBusyAction] = useState<string>();
  const [busyRowAction, setBusyRowAction] = useState<string>();
  const [submitLoading, setSubmitLoading] = useState(false);
  const [previewVisible, setPreviewVisible] = useState(false);
  const [previewText, setPreviewText] = useState('');
  const [previewTitle, setPreviewTitle] = useState('SeaTunnel 配置预览');
  const [checkVisible, setCheckVisible] = useState(false);
  const [cdcVisible, setCdcVisible] = useState(false);
  const [compatibilityVisible, setCompatibilityVisible] = useState(false);
  const [validationVisible, setValidationVisible] = useState(false);
  const [checkResult, setCheckResult] = useState<Awaited<ReturnType<typeof checkSyncTaskData>>['data']>();
  const [checkMode, setCheckMode] = useState<'COUNT' | 'KEY_RANGE'>('KEY_RANGE');
  const [checkBlockSize, setCheckBlockSize] = useState(10000);
  const [compatibilityResult, setCompatibilityResult] = useState<Awaited<ReturnType<typeof checkTargetCompatibility>>['data']>();
  const [validationResult, setValidationResult] = useState<Awaited<ReturnType<typeof validateSyncTask>>['data']>();
  const [metricsResult, setMetricsResult] = useState<SeaTunnelJobStatus>();

  const canAdd = hasPermi(userInfo, ['sync:task:add']);
  const canDetail = hasPermi(userInfo, ['sync:task:query']);
  const canEdit = hasPermi(userInfo, ['sync:task:edit']);
  const canRemove = hasPermi(userInfo, ['sync:task:remove']);
  const canValidate = hasPermi(userInfo, ['sync:task:validate']);
  const canCheck = hasPermi(userInfo, ['sync:task:check']);
  const canPreview = hasPermi(userInfo, ['sync:task:engine-config']);
  const canStart = hasPermi(userInfo, ['sync:task:start']);
  const canStatus = hasPermi(userInfo, ['sync:task:status']);
  const canPause = hasPermi(userInfo, ['sync:task:pause']);
  const canResume = hasPermi(userInfo, ['sync:task:resume']);
  const canStop = hasPermi(userInfo, ['sync:task:stop']);
  const canReinitialize = hasPermi(userInfo, ['sync:task:reinitialize']);
  const canCdcPrecheck = hasPermi(userInfo, ['sync:data-source:cdc-precheck']);

  useEffect(() => {
    listDataSources({ pageNum: 1, pageSize: 100 }).then(res => setDataSources(res.data?.rows || []));
    return () => {
      pollingTokenRef.current += 1;
    };
  }, []);

  const resetMetadataState = () => {
    setSourceDatabases([]);
    setSourceTables([]);
    setSourceDatabase('');
    setSourceMetadata(undefined);
    setCdcPrecheck(undefined);
    setSourceConnectionTest(undefined);
  };

  const resetTargetTables = () => {
    setTargetTables([]);
    setTargetTablesLoading(false);
    setTopicMode('EXISTING');
  };

  const loadTargetTables = async (targetId?: string | number) => {
    setTargetTables([]);
    if (!targetId) return;
    const target = dataSources.find(item => String(item.sourceId) === String(targetId));
    if (!target) return;
    setTargetTablesLoading(true);
    try {
      if (target.sourceType === 'KAFKA') {
        const result = await listKafkaTopics(targetId);
        setTargetTables((result.data || []).map(item => item.topic));
      } else {
        const result = await listDataSourceTables(targetId, target.databaseName);
        setTargetTables(result.data || []);
      }
    } finally {
      setTargetTablesLoading(false);
    }
  };

  const createTopic = async () => {
    const targetId = form.getFieldValue('targetId');
    const topic = form.getFieldValue('targetTable');
    if (!targetId || !topic) {
      message.error('请先填写 topic 名称');
      return;
    }
    setTopicCreateLoading(true);
    try {
      const result = await createKafkaTopic(targetId, { topic, partitions: 1, replicationFactor: 1 });
      form.setFieldValue('targetTable', result.data.topic);
      setTopicMode('EXISTING');
      await loadTargetTables(targetId);
      message.success(`topic ${result.data.topic} 创建成功`);
    } finally {
      setTopicCreateLoading(false);
    }
  };

  const loadSourceMetadata = async (sourceId?: string | number, tableName?: string) => {
    resetMetadataState();
    if (!sourceId) return;
    const source = dataSources.find(item => String(item.sourceId) === String(sourceId));
    if (!source) return;
    setMetadataLoading(true);
    try {
      const connectionResult = await testDataSource(sourceId);
      setSourceConnectionTest(connectionResult.data);
      if (!connectionResult.data?.success) return;
      if (source.sourceType === 'MYSQL') {
        const cdcResult = await checkDataSourceCdc(sourceId);
        setCdcPrecheck(cdcResult.data);
      }
      const databasesResult = await listDataSourceDatabases(sourceId);
      const databases = databasesResult.data || [];
      setSourceDatabases(databases);
      const database = source.databaseName || databases[0] || '';
      setSourceDatabase(database);
      if (!database) return;
      const tablesResult = await listDataSourceTables(sourceId, database);
      const tables = tablesResult.data || [];
      setSourceTables(tables);
      if (tableName) {
        const table = tableName.includes('.') ? tableName.slice(tableName.lastIndexOf('.') + 1) : tableName;
        if (table) {
          const metadataResult = await getDataSourceMetadata(sourceId, database, table);
          setSourceMetadata(metadataResult.data);
          if (!form.getFieldValue('selectedColumns')) form.setFieldValue('selectedColumns', metadataResult.data.columns.map(column => column.name));
          if (!form.getFieldValue('syncKeyColumns')) form.setFieldValue('syncKeyColumns', reliableKeyOptions(metadataResult.data)[0]?.value);
        }
      }
    } finally {
      setMetadataLoading(false);
    }
  };

  const loadTableMetadata = async (tableName?: string) => {
    setSourceMetadata(undefined);
    const sourceId = form.getFieldValue('sourceId');
    if (!sourceId || !sourceDatabase || !tableName) return;
    setMetadataLoading(true);
    try {
      const result = await getDataSourceMetadata(sourceId, sourceDatabase, tableName);
      setSourceMetadata(result.data);
      form.setFieldValue('selectedColumns', result.data.columns.map(column => column.name));
      form.setFieldValue('syncKeyColumns', reliableKeyOptions(result.data)[0]?.value);
      const targetId = form.getFieldValue('targetId');
      const target = dataSources.find(item => String(item.sourceId) === String(targetId));
      if (target?.sourceType === 'KAFKA' && !form.getFieldValue('targetTable')) {
        form.setFieldValue('targetTable', defaultKafkaTopic(sourceDatabase, tableName));
      }
    } finally {
      setMetadataLoading(false);
    }
  };

  const openAdd = () => {
    form.resetFields();
    form.setFieldsValue(defaultForm);
    resetMetadataState();
    resetTargetTables();
    setWizardStep(0);
    setModalTitle('新增同步任务');
    openModal();
  };

  const openEdit = async (row: SyncTaskVO) => {
    if (detailOpen) closeDetail();
    const res = await getSyncTask(row.taskId);
    form.resetFields();
    form.setFieldsValue({ ...res.data, selectedColumns: typeof res.data.selectedColumns === 'string' ? res.data.selectedColumns.split(',').filter(Boolean) : res.data.selectedColumns });
    void loadSourceMetadata(res.data.sourceId, res.data.sourceTable);
    void loadTargetTables(res.data.targetId);
    setWizardStep(0);
    setModalTitle('修改同步任务');
    openModal();
  };

  const submitForm = async (values: SyncTaskForm) => {
    // Step panels unmount their controls; use the full form store so preserved
    // values from earlier steps are included in the final payload.
    const allValues = { ...form.getFieldsValue(true), ...values } as SyncTaskForm;
    const selectedColumns = Array.isArray(allValues.selectedColumns)
      ? allValues.selectedColumns
      : String(allValues.selectedColumns || '').split(',').filter(Boolean);
    if (!allValues.sourceId || !allValues.targetId || !allValues.sourceTable || !allValues.targetTable || selectedColumns.length === 0 || !allValues.syncKeyColumns) {
      message.error('请返回前面步骤，补全源数据源、源表、目标表、字段和同步键');
      return false;
    }
    if (allValues.syncMode === 'INCREMENTAL' && !cdcPrecheck?.passed) {
      message.error('纯增量任务必须先通过 CDC 前置检查，请返回第一步重新选择并检查源数据源');
      return false;
    }
    const target = dataSources.find(item => String(item.sourceId) === String(allValues.targetId));
    const payload = { ...allValues, selectedColumns: selectedColumns.join(','), targetSchema: target?.sourceType === 'POSTGRESQL' ? allValues.targetSchema : undefined };
    setSubmitLoading(true);
    try {
      if (allValues.taskId) await updateSyncTask(payload);
      else await addSyncTask(payload);
      message.success(allValues.taskId ? '任务保存成功' : '任务创建成功');
      closeModal();
      actionRef.current?.reload();
      return true;
    } finally {
      setSubmitLoading(false);
    }
  };

  const nextWizardStep = async () => {
    const fieldsByStep = [
      ['sourceId', 'targetId'],
      ['sourceTable'],
      ['targetSchema', 'targetTable', 'fullDataMode'],
      ['selectedColumns', 'syncKeyColumns']
    ];
    const fields = fieldsByStep[wizardStep];
    if (fields) {
      await form.validateFields(fields);
      if (wizardStep === 0 && !sourceConnectionTest?.success) {
        message.error('请先选择可用的 MySQL 源数据源，并等待连接检查通过');
        return;
      }
      if (wizardStep === 1 && !sourceMetadata) {
        message.error('请等待源表元数据加载完成后继续');
        return;
      }
    }
    setWizardStep(current => Math.min(current + 1, 4));
  };

  const remove = async (row: SyncTaskVO) => {
    setBusyAction('delete');
    try {
      await deleteSyncTask(row.taskId);
      message.success('删除成功');
      closeDetail();
      actionRef.current?.reloadAndRest?.();
    } finally {
      setBusyAction(undefined);
    }
  };

  const loadDetail = async (taskId: string | number, showLoading = true) => {
    if (showLoading) setDetailLoading(true);
    try {
      const result = await getSyncTask(taskId);
      setSelectedTask(result.data);
      return result.data;
    } finally {
      if (showLoading) setDetailLoading(false);
    }
  };

  const openDetail = async (row: SyncTaskVO) => {
    pollingTokenRef.current += 1;
    setSelectedTask(row);
    setDetailOpen(true);
    await loadDetail(row.taskId);
  };

  const refreshListAndDetail = async (taskId: string | number) => {
    await loadDetail(taskId, false);
    actionRef.current?.reload();
  };

  const pollStatus = async (taskId: string | number, pendingStatuses: string[]) => {
    const token = pollingTokenRef.current;
    for (let attempt = 0; attempt < 15; attempt += 1) {
      await new Promise(resolve => window.setTimeout(resolve, 2000));
      if (token !== pollingTokenRef.current) return;
      try {
        const result = await refreshSyncTaskStatus(taskId);
        await refreshListAndDetail(taskId);
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
    if (!selectedTask || busyAction) return;
    const taskId = selectedTask.taskId;
    setBusyAction(key);
    try {
      const result = await action(taskId);
      message.success(result.data?.message || '操作成功');
      await refreshListAndDetail(taskId);
      if (pendingStatuses.length > 0) await pollStatus(taskId, pendingStatuses);
    } catch {
      await loadDetail(taskId, false);
    } finally {
      setBusyAction(undefined);
    }
  };

  const runRowAction = async (
    row: SyncTaskVO,
    action: 'stop' | 'delete',
    request: (id: string | number) => Promise<{ data?: unknown }>
  ) => {
    const actionKey = `${row.taskId}:${action}`;
    if (busyRowAction) return;
    setBusyRowAction(actionKey);
    try {
      const result = await request(row.taskId);
      const resultMessage = (result.data as { message?: string } | undefined)?.message;
      message.success(resultMessage || (action === 'stop' ? '任务已中止' : '删除成功'));
      actionRef.current?.reload();
    } finally {
      setBusyRowAction(undefined);
    }
  };

  const validate = async () => {
    if (!selectedTask) return;
    setBusyAction('validate');
    setValidationResult(undefined);
    try {
      const result = await validateSyncTask(selectedTask.taskId);
      setValidationResult(result.data);
      setValidationVisible(true);
      setCdcPrecheck(result.data.cdcPrecheck);
      setCdcVisible(Boolean(result.data.cdcPrecheck));
      setCompatibilityResult(result.data.targetCompatibility);
      setCompatibilityVisible(Boolean(result.data.targetCompatibility));
      message[result.data.valid ? 'success' : 'error'](result.data.message);
      await refreshListAndDetail(selectedTask.taskId);
    } finally {
      setBusyAction(undefined);
    }
  };

  const runCdcPrecheck = async () => {
    if (!selectedTask?.sourceId) return;
    setBusyAction('cdc-precheck');
    try {
      const result = await checkDataSourceCdc(selectedTask.sourceId);
      setCdcPrecheck(result.data);
      setCdcVisible(true);
      message[result.data?.passed ? 'success' : 'error'](result.data?.message || 'CDC 前置检查完成');
    } finally {
      setBusyAction(undefined);
    }
  };

  const runTargetCompatibility = async () => {
    if (!selectedTask) return;
    setBusyAction('target-compatibility');
    try {
      const result = await checkTargetCompatibility(selectedTask.taskId);
      setCompatibilityResult(result.data);
      setCompatibilityVisible(true);
      message[result.data?.passed ? 'success' : 'error'](result.data?.message || '目标兼容性检查完成');
    } finally {
      setBusyAction(undefined);
    }
  };

  const checkData = async () => {
    if (!selectedTask) return;
    setBusyAction('check');
    try {
      const result = await checkSyncTaskData(selectedTask.taskId, { mode: checkMode, blockSize: checkBlockSize, strictWatermark: true });
      setCheckResult(result.data);
      setCheckVisible(true);
    } finally {
      setBusyAction(undefined);
    }
  };

  const previewConfig = async () => {
    if (!selectedTask) return;
    setBusyAction('engine-config');
    try {
      const result = await previewSyncTaskConfig(selectedTask.taskId);
      setPreviewTitle(`${result.data?.jobName || selectedTask.taskName} 配置预览`);
      setPreviewText(result.data?.config || '');
      setPreviewVisible(true);
    } finally {
      setBusyAction(undefined);
    }
  };

  const refreshStatus = async () => {
    if (!selectedTask) return;
    setBusyAction('status');
    try {
      const result = await refreshSyncTaskStatus(selectedTask.taskId);
      setMetricsResult(result.data);
      message.success(`引擎状态：${result.data?.engineStatus || result.data?.status || '-'}`);
      await refreshListAndDetail(selectedTask.taskId);
    } finally {
      setBusyAction(undefined);
    }
  };

  const closeDetail = () => {
    pollingTokenRef.current += 1;
    setDetailOpen(false);
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
  };

  const sourceOptions = dataSources.filter(item => item.sourceType === 'MYSQL').map(item => ({ label: sourceLabel(item), value: item.sourceId }));
  const targetOptions = dataSources
    .filter(item => item.sourceType === 'POSTGRESQL' || item.sourceType === 'MYSQL' || item.sourceType === 'KAFKA')
    .map(item => ({ label: sourceLabel(item), value: item.sourceId }));
  const selectedTargetType = dataSources.find(item => String(item.sourceId) === String(selectedTask?.targetId))?.sourceType;
  const selectedTargetIsKafka = selectedTargetType === 'KAFKA';
  const selectedTargetIsPostgres = selectedTargetType === 'POSTGRESQL';
  const taskStatus = selectedTask?.status || '';
  const actionDisabled = Boolean(busyAction);
  const configLocked = ['RUNNING', 'PAUSING'].includes(taskStatus);
  const rowActionDisabled = Boolean(busyRowAction);

  const columns: ProColumns<SyncTaskVO>[] = [
    { title: '任务名称', dataIndex: 'taskName', width: 190 },
    { title: '源表', dataIndex: 'sourceTable', width: 150, ellipsis: true },
    { title: '目标表', dataIndex: 'targetTable', width: 150, ellipsis: true },
    { title: '同步模式', dataIndex: 'syncMode', width: 140, search: false, valueEnum: { FULL: '全量', INCREMENTAL: '纯增量', FULL_CDC: '全量 + CDC' } },
    {
      title: '状态', dataIndex: 'status', width: 110,
      valueEnum: statusLabels,
      render: (_, row) => statusTag(row.status, row.lastError)
    },
    { title: '更新时间', dataIndex: 'updateTime', width: 170, search: false },
    {
      title: '操作', valueType: 'option', width: 240, fixed: 'right',
      render: (_, row) => {
        const canAbort = canStop && ['RUNNING', 'PAUSING', 'PAUSED'].includes(row.status || '') && Boolean(row.engineJobId);
        const canDelete = canRemove;
        const deleteDisabled = ['RUNNING', 'PAUSING'].includes(row.status || '');
        return (
          <Space size={0} wrap={false} style={{ gap: 0, whiteSpace: 'nowrap' }}>
            {canDetail && <Button type="link" style={{ paddingInline: 4 }} icon={<EyeOutlined />} disabled={rowActionDisabled} onClick={() => openDetail(row)}>详情</Button>}
            {canAbort && <Button type="link" danger style={{ paddingInline: 4 }} icon={<StopOutlined />} loading={busyRowAction === `${row.taskId}:stop`} disabled={rowActionDisabled} onClick={() => Modal.confirm({ title: '中止同步任务', content: `中止后任务将停止运行，是否确认中止“${row.taskName}”？`, okText: '确认中止', cancelText: '取消', onOk: () => runRowAction(row, 'stop', stopSyncTask) })}>中止</Button>}
            {canDelete && <Button type="link" danger style={{ paddingInline: 4 }} icon={<DeleteOutlined />} loading={busyRowAction === `${row.taskId}:delete`} disabled={rowActionDisabled || deleteDisabled} title={deleteDisabled ? '任务运行中不可删除' : undefined} onClick={() => Modal.confirm({ title: '删除同步任务', content: `是否确认删除任务“${row.taskName}”？`, okText: '确认删除', cancelText: '取消', onOk: () => runRowAction(row, 'delete', deleteSyncTask) })}>删除</Button>}
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
        toolBarRender={() => [canAdd && <Button key="add" type="primary" icon={<PlusOutlined />} onClick={openAdd}>新增任务</Button>]}
      />
      <Modal title={selectedTask ? `任务详情：${selectedTask.taskName}` : '任务详情'} open={detailOpen} width={960} footer={null} destroyOnHidden onCancel={closeDetail}>
        {selectedTask && (
          <Space direction="vertical" size={18} style={{ width: '100%' }}>
            <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }}>
              <Descriptions.Item label="任务状态">{statusTag(selectedTask.status, selectedTask.lastError)}</Descriptions.Item>
              <Descriptions.Item label="同步模式">{selectedTask.syncMode === 'FULL_CDC' ? '全量 + CDC' : selectedTask.syncMode || '-'}</Descriptions.Item>
              <Descriptions.Item label="调度模式">{selectedTask.scheduleMode === 'CRON' ? `定时 Cron${selectedTask.cronExpression ? ` (${selectedTask.cronExpression})` : ''}` : selectedTask.scheduleMode === 'REALTIME' ? '常驻实时' : selectedTask.scheduleMode === 'ONCE' ? '一次性' : '手动'}</Descriptions.Item>
              <Descriptions.Item label="下次执行">{selectedTask.nextRunTime || '-'}</Descriptions.Item>
              <Descriptions.Item label="SeaTunnel Job ID">{selectedTask.engineJobId || '-'}</Descriptions.Item>
              <Descriptions.Item label="源表">{selectedTask.sourceTable || '-'}</Descriptions.Item>
              <Descriptions.Item label="目标表">{selectedTargetIsPostgres ? `${selectedTask.targetSchema || 'public'}.${selectedTask.targetTable || '-'}` : (selectedTask.targetTable || '-')}</Descriptions.Item>
              <Descriptions.Item label="最近检查点">{selectedTask.lastCheckpointTime || '-'}</Descriptions.Item>
              <Descriptions.Item label="最近核对">{selectedTask.lastCheckTime ? `${selectedTask.lastCheckTime} / ${selectedTask.lastCheckMatched === '1' ? '一致' : '不一致'}` : '未核对'}</Descriptions.Item>
              {selectedTargetIsKafka && <Descriptions.Item label="Kafka 已发布事件">{selectedTask.kafkaPublishedCount ?? '-'}</Descriptions.Item>}
              {selectedTargetIsKafka && <Descriptions.Item label="Kafka 最近分区/offset">{selectedTask.kafkaLastPartition == null ? '-' : `${selectedTask.kafkaLastPartition} / ${selectedTask.kafkaLastOffset ?? '-'}`}</Descriptions.Item>}
              {selectedTargetIsKafka && <Descriptions.Item label="Kafka 最近源事件时间">{selectedTask.kafkaLastSourceEventTime || '-'}</Descriptions.Item>}
              {selectedTargetIsKafka && <Descriptions.Item label="Kafka 最近 broker 确认">{selectedTask.kafkaLastBrokerAckTime || '-'}</Descriptions.Item>}
              {selectedTargetIsKafka && <Descriptions.Item label="Kafka 发布延迟">{selectedTask.kafkaLagSeconds == null ? '-' : `${selectedTask.kafkaLagSeconds} 秒`}</Descriptions.Item>}
              <Descriptions.Item label="最大行数/秒">{selectedTask.readLimitRowsPerSecond ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="最大字节/秒">{selectedTask.readLimitBytesPerSecond ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="快照并行度">{selectedTask.snapshotParallelism ?? '-'}</Descriptions.Item>
              <Descriptions.Item label="CDC 连接池上限">{selectedTask.sourceConnectionLimit ?? '-'}</Descriptions.Item>
            </Descriptions>
            {metricsResult && <Alert type="info" showIcon message="实时运行指标" description={<Descriptions size="small" column={{ xs: 1, sm: 2, md: 3 }}>
              <Descriptions.Item label="当前阶段">{metricsResult.phase === 'SNAPSHOT' ? '全量快照' : 'CDC 增量'}</Descriptions.Item>
              <Descriptions.Item label="源端已读取">{metricsResult.sourceReceivedCount ?? '-'} 行 / {metricsResult.sourceReceivedBytes ?? '-'} 字节</Descriptions.Item>
              <Descriptions.Item label="目标已提交">{metricsResult.sinkCommittedCount ?? '-'} 行 / {metricsResult.sinkCommittedBytes ?? '-'} 字节</Descriptions.Item>
              <Descriptions.Item label="源端吞吐">{metricsResult.sourceQps == null ? '-' : `${metricsResult.sourceQps.toFixed(2)} 行/秒`}</Descriptions.Item>
              <Descriptions.Item label="目标吞吐">{metricsResult.sinkQps == null ? '-' : `${metricsResult.sinkQps.toFixed(2)} 行/秒`}</Descriptions.Item>
              <Descriptions.Item label="CDC 延迟">{metricsResult.cdcLagSeconds == null ? (metricsResult.metricsMessage || '暂不可计算') : `${metricsResult.cdcLagSeconds} 秒`}</Descriptions.Item>
            </Descriptions>} />}
            {selectedTask.lastError && <Alert type="error" showIcon message="最近一次错误" description={<Typography.Text copyable>{selectedTask.lastError}</Typography.Text>} />}
            {selectedTask.lastSkipReason && <Alert type="warning" showIcon message="最近一次调度未执行" description={selectedTask.lastSkipReason} />}
            <Divider titlePlacement="left" plain>准备与诊断</Divider>
            <Space wrap>
              {canCdcPrecheck && selectedTask.syncMode === 'FULL_CDC' && <Button icon={<SafetyCertificateOutlined />} loading={busyAction === 'cdc-precheck'} disabled={actionDisabled} onClick={runCdcPrecheck}>CDC 前置检查</Button>}
              {canValidate && <Button icon={<SafetyCertificateOutlined />} loading={busyAction === 'target-compatibility'} disabled={actionDisabled} onClick={runTargetCompatibility}>目标兼容性</Button>}
              {canValidate && <Button icon={<SafetyCertificateOutlined />} loading={busyAction === 'validate'} disabled={actionDisabled} onClick={validate}>校验任务</Button>}
              {canCheck && <Space.Compact>
                <Select aria-label="数据核对模式" value={checkMode} onChange={value => setCheckMode(value)} options={[{ label: '同步键分块', value: 'KEY_RANGE' }, { label: '整表行数', value: 'COUNT' }]} disabled={actionDisabled} style={{ width: 120 }} />
                {checkMode === 'KEY_RANGE' && <InputNumber aria-label="核对分块步长" min={1} max={1000000} value={checkBlockSize} onChange={value => setCheckBlockSize(value || 10000)} disabled={actionDisabled} style={{ width: 110 }} />}
                <Button icon={<CheckCircleOutlined />} loading={busyAction === 'check'} disabled={actionDisabled} onClick={checkData}>数据核对</Button>
              </Space.Compact>}
              {canPreview && <Button icon={<CodeOutlined />} loading={busyAction === 'engine-config'} disabled={actionDisabled} onClick={previewConfig}>查看引擎配置</Button>}
              {canStatus && selectedTask.engineJobId && <Button icon={<ReloadOutlined />} loading={busyAction === 'status'} disabled={actionDisabled} onClick={refreshStatus}>刷新状态</Button>}
            </Space>
            {cdcPrecheck && cdcVisible && <Alert
              style={{ width: '100%' }}
              type={cdcPrecheck.passed ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage(cdcPrecheck.message, () => setCdcVisible(false))}
              description={<Descriptions size="small" column={{ xs: 1, sm: 2 }}>
                <Descriptions.Item label="server-id">{cdcPrecheck.serverId || '-'}</Descriptions.Item>
                <Descriptions.Item label="GTID 模式">{cdcPrecheck.gtidMode || '-'}</Descriptions.Item>
                <Descriptions.Item label="binlog 保留">{cdcPrecheck.binlogRetention || '-'}</Descriptions.Item>
                {cdcPrecheck.checks?.map(item => <Descriptions.Item key={item.code} label={item.label}>
                  <Tag color={item.passed ? 'success' : item.required ? 'error' : 'warning'}>{item.passed ? '通过' : '未通过'}</Tag> {item.message}{item.suggestion ? `；处理建议：${item.suggestion}` : ''}
                </Descriptions.Item>)}
              </Descriptions>}
            />}
            {compatibilityResult && compatibilityVisible && <Alert
              style={{ width: '100%' }}
              type={compatibilityResult.passed ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage(compatibilityResult.message, () => setCompatibilityVisible(false))}
              description={<Descriptions size="small" column={{ xs: 1, sm: 2 }}>
                <Descriptions.Item label="源表">{compatibilityResult.sourceTable}</Descriptions.Item>
                <Descriptions.Item label="目标表">{compatibilityResult.targetTable}</Descriptions.Item>
                {compatibilityResult.checks?.map(item => <Descriptions.Item key={item.code} label={item.label}>
                  <Tag color={item.passed ? 'success' : item.required ? 'error' : 'warning'}>{item.passed ? '通过' : '未通过'}</Tag> {item.actual || '-'} {item.message}{item.suggestion ? `；处理建议：${item.suggestion}` : ''}
                </Descriptions.Item>)}
              </Descriptions>}
            />}
            {validationResult && validationVisible && <Alert
              style={{ width: '100%' }}
              type={validationResult.valid ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage(validationResult.message, () => setValidationVisible(false))}
              description={<Descriptions size="small" column={{ xs: 1, sm: 2 }}>
                <Descriptions.Item label="源连接">{validationResult.source.success ? '通过' : validationResult.source.message}</Descriptions.Item>
                <Descriptions.Item label="目标连接">{validationResult.target.success ? '通过' : validationResult.target.message}</Descriptions.Item>
                {validationResult.cdcPrecheck && <Descriptions.Item label="CDC 前置检查">{validationResult.cdcPrecheck.passed ? '通过' : validationResult.cdcPrecheck.message}</Descriptions.Item>}
                {validationResult.targetCompatibility && <Descriptions.Item label="目标表兼容性">{validationResult.targetCompatibility.passed ? '通过' : validationResult.targetCompatibility.message}</Descriptions.Item>}
              </Descriptions>}
            />}
            {checkVisible && checkResult && <Alert
              style={{ width: '100%' }}
              type={checkResult.success && checkResult.matched ? 'success' : 'error'}
              showIcon
              message={diagnosticMessage('数据核对结果', () => setCheckVisible(false))}
              description={<Space direction="vertical" style={{ width: '100%' }}>
                <Descriptions size="small" column={1}>
                  <Descriptions.Item label="源表">{checkResult.sourceTable}</Descriptions.Item>
                  <Descriptions.Item label="目标表">{checkResult.targetTable}</Descriptions.Item>
                  <Descriptions.Item label="源端行数">{checkResult.sourceRows ?? '-'}</Descriptions.Item>
                  <Descriptions.Item label="目标端行数">{checkResult.targetRows ?? '-'}</Descriptions.Item>
                  <Descriptions.Item label="差异">{checkResult.difference ?? '-'}</Descriptions.Item>
                  <Descriptions.Item label="核对模式">{checkResult.checkMode === 'KEY_RANGE' ? `同步键分块（步长 ${checkResult.blockSize ?? '-'}）` : '整表行数'}</Descriptions.Item>
                  <Descriptions.Item label="分块统计">{checkResult.totalBlocks == null ? '-' : `共 ${checkResult.totalBlocks} 块，通过 ${checkResult.matchedBlocks ?? 0}，不一致 ${checkResult.mismatchedBlocks ?? 0}，失败 ${checkResult.failedBlocks ?? 0}`}</Descriptions.Item>
                  <Descriptions.Item label="结论"><Tag color={checkResult.success && checkResult.matched ? 'success' : 'error'}>{checkResult.message}</Tag></Descriptions.Item>
                  <Descriptions.Item label="水位提示">{checkResult.watermarkMessage || (selectedTask.syncMode === 'FULL_CDC' ? '严格核对要求任务已暂停；当前结果不代表持续 CDC 的实时同水位一致性。' : '静态任务可作为当前快照核对结果。')}</Descriptions.Item>
                  <Descriptions.Item label="记录时间">{new Date().toLocaleString()}</Descriptions.Item>
                </Descriptions>
                {checkResult.blocks && checkResult.blocks.length > 0 && <Table size="small" pagination={{ pageSize: 5 }} rowKey="index" dataSource={checkResult.blocks} columns={[{ title: '块', dataIndex: 'index', width: 55 }, { title: '范围', render: (_, block) => `[${block.lowerBound}, ${block.upperBound})` }, { title: '源端', dataIndex: 'sourceRows' }, { title: '目标端', dataIndex: 'targetRows' }, { title: '差异', dataIndex: 'difference' }, { title: '结果', render: (_, block) => <Tag color={block.success && block.matched ? 'success' : 'error'}>{block.message || (block.matched ? '一致' : '不一致')}</Tag> }]} />}
              </Space>}
            />}
            {previewVisible && <Alert
              style={{ width: '100%' }}
              type="success"
              showIcon
              message={diagnosticMessage(previewTitle, () => setPreviewVisible(false))}
              description={<Typography.Paragraph copyable={{ text: previewText }} style={{ width: '100%', maxHeight: 360, overflow: 'auto', whiteSpace: 'pre-wrap', fontFamily: 'monospace', marginBottom: 0 }}>{previewText || '暂无配置内容'}</Typography.Paragraph>}
            />}
            <Divider titlePlacement="left" plain>运行控制</Divider>
            <Space wrap>
              {canStart && ['DRAFT', 'STOPPED', 'FINISHED'].includes(taskStatus) && <Button type="primary" icon={<PlayCircleOutlined />} loading={busyAction === 'start'} disabled={actionDisabled} onClick={() => runAction('start', startSyncTask)}>启动任务</Button>}
              {canResume && ['PAUSED', 'FAILED'].includes(taskStatus) && <Button type="primary" icon={<PlayCircleOutlined />} loading={busyAction === 'resume'} disabled={actionDisabled} onClick={() => runAction('resume', resumeSyncTask)}>恢复任务</Button>}
              {canReinitialize && ['REINITIALIZE_REQUIRED', 'FAILED'].includes(taskStatus) && <Button type="primary" danger icon={<ReloadOutlined />} loading={busyAction === 'reinitialize'} disabled={actionDisabled} onClick={() => Modal.confirm({ title: '重新初始化同步任务', content: selectedTask.syncMode === 'FULL_CDC' ? '此操作会丢弃旧 checkpoint/savepoint，重新执行全量并继续 CDC。目标端将按当前全量数据模式处理，是否继续？' : '此操作会丢弃旧恢复状态并重新执行全量，是否继续？', okText: '确认重新初始化', cancelText: '取消', onOk: () => runAction('reinitialize', reinitializeSyncTask) })}>重新初始化</Button>}
              {canPause && taskStatus === 'RUNNING' && <Button icon={<PauseCircleOutlined />} loading={busyAction === 'pause'} disabled={actionDisabled} onClick={() => runAction('pause', pauseSyncTask, ['PAUSING'])}>暂停任务</Button>}
              {canStop && ['RUNNING', 'PAUSING', 'PAUSED', 'FAILED'].includes(taskStatus) && selectedTask.engineJobId && <Button danger icon={<StopOutlined />} loading={busyAction === 'stop'} disabled={actionDisabled} onClick={() => Modal.confirm({ title: '中止同步任务', content: '中止后不会保留可恢复状态，是否继续？', okText: '确认中止', cancelText: '取消', onOk: () => runAction('stop', stopSyncTask) })}>中止任务</Button>}
            </Space>
            <Divider titlePlacement="left" plain>配置管理</Divider>
            <Space wrap>
              {canEdit && <Button icon={<EditOutlined />} disabled={actionDisabled || configLocked} title={configLocked ? '任务运行中不可修改' : undefined} onClick={() => openEdit(selectedTask)}>修改任务</Button>}
              {canRemove && <Button danger icon={<DeleteOutlined />} loading={busyAction === 'delete'} disabled={actionDisabled || configLocked} title={configLocked ? '任务运行中不可删除' : undefined} onClick={() => Modal.confirm({ title: '删除同步任务', content: `是否确认删除任务“${selectedTask.taskName}”？`, okText: '确认删除', cancelText: '取消', onOk: () => remove(selectedTask) })}>删除任务</Button>}
            </Space>
          </Space>
        )}
        {detailLoading && <Typography.Text type="secondary">正在加载最新任务信息...</Typography.Text>}
      </Modal>
      <ModalForm<SyncTaskForm> title={modalTitle} open={modalOpen} form={form} preserve layout="vertical" width={760} modalProps={{ destroyOnHidden: true, onCancel: () => { resetMetadataState(); resetTargetTables(); closeModal(); } }} onOpenChange={open => { if (!open) { resetMetadataState(); resetTargetTables(); closeModal(); } }} onFinish={submitForm} onFinishFailed={() => message.error('请先完善当前步骤的必填项')} submitter={{ render: () => <Space><Button onClick={() => { resetMetadataState(); resetTargetTables(); closeModal(); }}>取消</Button>{wizardStep > 0 && <Button onClick={() => setWizardStep(current => current - 1)}>上一步</Button>}{wizardStep < 4 ? <Button type="primary" onClick={() => void nextWizardStep()}>下一步</Button> : <Button type="primary" loading={submitLoading} onClick={() => form.submit()}>{form.getFieldValue('taskId') ? '确认保存' : '确认创建'}</Button>}</Space> }}>
        <ProFormText name="taskId" hidden />
        <Steps current={wizardStep} size="small" style={{ marginBottom: 24 }} items={[{ title: '数据源' }, { title: '同步粒度' }, { title: '目标端' }, { title: '字段映射' }, { title: '同步方式' }]} />
        {wizardStep === 0 && <><Alert type="info" showIcon message="选择数据源后会重新连接并探查元数据，不直接信任历史连接状态。" style={{ marginBottom: 16 }} /><ProFormSelect name="sourceId" label="源数据源（MySQL）" options={sourceOptions} rules={[{ required: true, message: '请选择源数据源' }]} fieldProps={{ loading: dataSources.length === 0, onChange: value => void loadSourceMetadata(value as string | number) }} />{sourceConnectionTest && <Alert type={sourceConnectionTest.success ? 'success' : 'error'} showIcon message={sourceConnectionTest.success ? `源端连接可用（${sourceConnectionTest.latencyMs} ms）` : '源端连接失败'} description={sourceConnectionTest.message} style={{ marginBottom: 12 }} />}{cdcPrecheck && <Alert type={cdcPrecheck.passed ? 'success' : 'warning'} showIcon message={cdcPrecheck.passed ? 'CDC 前置检查通过' : 'CDC 前置检查未通过'} description={cdcPrecheck.message} style={{ marginBottom: 12 }} />}<Form.Item label="源数据库"><Typography.Text>{sourceDatabase || '选择源数据源后自动读取'}</Typography.Text>{sourceDatabases.length > 1 && <Typography.Text type="secondary"> 已探查 {sourceDatabases.length} 个数据库，当前单表任务使用数据源配置的默认数据库。</Typography.Text>}</Form.Item><ProFormSelect name="targetId" label="目标数据源（MySQL / PostgreSQL / Kafka）" options={targetOptions} rules={[{ required: true, message: '请选择目标数据源' }]} fieldProps={{ onChange: value => { form.setFieldValue('targetTable', undefined); void loadTargetTables(value as string | number); } }} /></>}
        {wizardStep === 1 && <><Alert type="info" showIcon message="当前入口创建单表任务；多表和整库同步请在“同步任务组”页面创建。" style={{ marginBottom: 16 }} /><ProFormSelect name="sourceTable" label="源表名" options={sourceTables.map(table => ({ label: table, value: table }))} showSearch rules={[{ required: true, message: '请选择源表' }]} fieldProps={{ loading: metadataLoading && sourceTables.length === 0, onChange: value => void loadTableMetadata(value as string) }} />{sourceMetadata && <Alert type={sourceMetadata.primaryKeys.length > 0 || sourceMetadata.uniqueKeys.some(item => item.allNotNull) ? 'success' : 'warning'} showIcon message={`已读取 ${sourceMetadata.tableName} 元数据`} description={<Descriptions size="small" column={{ xs: 1, sm: 2 }}><Descriptions.Item label="字段数">{sourceMetadata.columns.length}</Descriptions.Item><Descriptions.Item label="字符集">{sourceMetadata.charset || '-'}</Descriptions.Item><Descriptions.Item label="主键">{sourceMetadata.primaryKeys.join(', ') || '无'}</Descriptions.Item><Descriptions.Item label="可靠唯一键">{sourceMetadata.uniqueKeys.filter(item => item.allNotNull).map(item => `${item.name} (${item.columns.join(', ')})`).join('; ') || '无'}</Descriptions.Item></Descriptions>} />}</>}
        {wizardStep === 2 && <><Form.Item noStyle shouldUpdate={(prev, current) => prev.targetId !== current.targetId}>{({ getFieldValue }) => getFieldValue('targetId') && dataSources.find(item => String(item.sourceId) === String(getFieldValue('targetId')))?.sourceType === 'KAFKA' ? <><Form.Item label="Kafka topic 来源"><Radio.Group value={topicMode} onChange={event => setTopicMode(event.target.value)} options={[{ label: '选择已有 topic', value: 'EXISTING' }, { label: '创建新 topic', value: 'CREATE' }]} /></Form.Item><Form.Item name="targetTable" preserve label="目标 topic" rules={[{ required: true, message: '请选择或填写目标 topic' }]} extra={topicMode === 'CREATE' ? '平台将使用 1 个分区、1 个副本创建 topic，不会覆盖已存在的 topic。' : '仅展示当前账号可见的 topic。'}><AutoComplete options={targetTables.map(table => ({ label: table, value: table }))} allowClear placeholder={topicMode === 'CREATE' ? '输入新 topic 名称' : (targetTablesLoading ? '正在读取 topic 列表' : '选择已有 topic')} /></Form.Item>{topicMode === 'CREATE' && <Button type="primary" loading={topicCreateLoading} onClick={() => void createTopic()}>创建 topic</Button>}<Alert type="info" showIcon message="Kafka 任务使用 JSON 事件信封；创建 topic 需要 Kafka 凭证具备 Create 权限。" /></> : <><ProFormText name="targetSchema" label="目标 Schema（仅 PostgreSQL）" placeholder="默认 public" /><Form.Item name="targetTable" preserve label="目标表名" rules={[{ required: true, message: '请选择已有表或输入目标表名' }]} extra="MySQL/PostgreSQL 可选择已有表或输入新表名。"><AutoComplete options={targetTables.map(table => ({ label: table, value: table }))} allowClear placeholder={targetTablesLoading ? '正在读取目标端表列表，可直接输入新表名' : '选择已有表或输入目标表名'} /></Form.Item><Alert type="info" showIcon message="关系型目标已有表会执行兼容性检查；新表在任务启动时自动创建。" /></>}</Form.Item></>}
        {wizardStep === 3 && sourceMetadata && <>{mappingRisks(sourceMetadata).map(risk => <Alert key={risk} type="warning" showIcon message={risk} style={{ marginBottom: 8 }} />)}<Form.Item name="selectedColumns" label="纳入同步的字段" rules={[{ required: true, message: '至少选择一个字段' }]} extra="MVP 仅支持同名字段映射；同步键字段不可排除。"><Checkbox.Group options={sourceMetadata.columns.map(column => ({ label: `${column.name} (${column.typeName || '-'})`, value: column.name, disabled: (form.getFieldValue('syncKeyColumns') || '').split(',').includes(column.name) }))} /></Form.Item><Form.Item name="syncKeyColumns" label="同步键" rules={[{ required: true, message: '请选择可靠同步键' }]} extra="优先使用主键；没有主键时仅可选择所有字段均为非空的唯一键。"><Select options={reliableKeyOptions(sourceMetadata)} disabled={reliableKeyOptions(sourceMetadata).length === 0} placeholder={reliableKeyOptions(sourceMetadata).length ? '请选择同步键' : '源表没有可靠同步键'} /></Form.Item>{reliableKeyOptions(sourceMetadata).length === 0 && <Alert type="warning" showIcon message="该表没有可靠同步键，只能创建全量任务；增量相关模式在保存时会被阻断。" />}</>}
        {wizardStep === 4 && <><ProFormText name="taskName" label="任务名称" rules={[{ required: true, message: '请输入任务名称' }]} /><ProFormSelect name="syncMode" label="同步模式" options={[{ label: '全量同步', value: 'FULL' }, { label: '纯增量', value: 'INCREMENTAL' }, { label: '全量 + CDC', value: 'FULL_CDC' }]} rules={[{ required: true }]} /><Form.Item noStyle shouldUpdate={(prev, current) => prev.syncMode !== current.syncMode || prev.incrementalStartupMode !== current.incrementalStartupMode}>{({ getFieldValue }) => getFieldValue('syncMode') === 'INCREMENTAL' ? <><Alert type="warning" showIcon message="纯增量不会补齐任务创建前的历史数据，目标端必须已有可信基线。" style={{ marginBottom: 12 }} />{cdcPrecheck && !cdcPrecheck.passed && <Alert type="error" showIcon message="CDC 前置检查未通过，不能创建纯增量任务。" description={cdcPrecheck.message} style={{ marginBottom: 12 }} />}<ProFormSelect name="incrementalStartupMode" label="增量启动位点" options={[{ label: '从创建后的最新位点开始', value: 'LATEST' }, { label: '按指定时间开始', value: 'TIMESTAMP' }, { label: '指定 binlog 文件和位置', value: 'SPECIFIC' }]} rules={[{ required: true }]} />{getFieldValue('incrementalStartupMode') === 'TIMESTAMP' && <ProFormText name="incrementalStartupTimestamp" label="启动时间" placeholder="例如 2026-08-27T12:30:00" fieldProps={{ type: 'datetime-local' }} rules={[{ required: true, message: '请选择启动时间' }]} extra="按源端 Asia/Shanghai 时区换算为 SeaTunnel 毫秒时间戳。" />}{getFieldValue('incrementalStartupMode') === 'SPECIFIC' && <Space align="start"><ProFormText name="incrementalStartupBinlogFile" label="binlog 文件" placeholder="mysql-bin.000001" rules={[{ required: true, message: '请输入 binlog 文件' }]} fieldProps={{ style: { width: 250 } }} /><ProFormDigit name="incrementalStartupBinlogPosition" label="binlog 位置" min={4} rules={[{ required: true, message: '请输入 binlog 位置' }]} fieldProps={{ style: { width: 180 } }} /></Space>}</> : null}</Form.Item><ProFormSelect name="ddlPolicy" label="DDL 策略" options={[{ label: '失败即停', value: 'FAIL' }, { label: '忽略', value: 'IGNORE' }]} rules={[{ required: true }]} /><ProFormSelect name="scheduleMode" label="调度模式" options={[{ label: '保存后执行一次', value: 'ONCE' }, { label: 'Cron 定时执行', value: 'CRON' }, { label: '常驻实时（手动启动）', value: 'REALTIME' }, { label: '手动（不自动触发）', value: 'MANUAL' }]} rules={[{ required: true }]} /><Form.Item noStyle shouldUpdate={(prev, current) => prev.scheduleMode !== current.scheduleMode}>{({ getFieldValue }) => getFieldValue('scheduleMode') === 'CRON' ? <ProFormText name="cronExpression" label="Cron 表达式" placeholder="例如 0 0 2 * * ?（秒 分 时 日 月 周）" rules={[{ required: true, message: '请输入 Cron 表达式' }]} extra="使用 Spring 6 位 Cron，保存时会校验并计算下次执行时间。" /> : null}</Form.Item><Form.Item noStyle shouldUpdate={(prev, current) => prev.fullDataMode !== current.fullDataMode || prev.syncMode !== current.syncMode}>{({ getFieldValue }) => getFieldValue('fullDataMode') === 'OVERWRITE' && getFieldValue('syncMode') === 'FULL_CDC' ? <Alert type="warning" showIcon message="全量 + CDC 的覆盖刷新需要一致性切换水位，当前 MVP 请使用合并（upsert）或创建纯全量覆盖任务。" /> : getFieldValue('fullDataMode') === 'OVERWRITE' && getFieldValue('syncMode') !== 'INCREMENTAL' ? <Alert type="warning" showIcon message="覆盖刷新将先写入临时表，作业成功后才替换正式表。" /> : null}</Form.Item><Divider titlePlacement="left" plain>源库保护</Divider><Space wrap align="start"><ProFormDigit name="readLimitRowsPerSecond" label="最大行数/秒" min={1} max={100000} rules={[{ required: true }]} fieldProps={{ style: { width: 180 }}} /><ProFormDigit name="readLimitBytesPerSecond" label="最大字节/秒" min={1} max={1073741824} rules={[{ required: true }]} fieldProps={{ style: { width: 210 }}} /><ProFormDigit name="snapshotParallelism" label="快照并行度" min={1} max={4} rules={[{ required: true }]} fieldProps={{ style: { width: 150 }}} /><ProFormDigit name="sourceConnectionLimit" label="CDC 连接池上限" min={1} max={8} rules={[{ required: true }]} fieldProps={{ style: { width: 180 }}} /></Space><Divider titlePlacement="left" plain>提交确认</Divider><Form.Item noStyle shouldUpdate>{({ getFieldsValue }) => { const values = getFieldsValue(true); const isTargetPostgres = dataSources.find(item => String(item.sourceId) === String(values.targetId))?.sourceType === 'POSTGRESQL'; return <Descriptions size="small" column={2} bordered><Descriptions.Item label="源端">{sourceDatabase || '-'} / {values.sourceTable || '-'}</Descriptions.Item><Descriptions.Item label="目标端">{isTargetPostgres ? `${values.targetSchema || 'public'} / ` : ''}{values.targetTable || '-'}</Descriptions.Item><Descriptions.Item label="同步模式">{values.syncMode || '-'}</Descriptions.Item><Descriptions.Item label="同步键">{values.syncKeyColumns || '-'}</Descriptions.Item><Descriptions.Item label="字段数">{Array.isArray(values.selectedColumns) ? values.selectedColumns.length : String(values.selectedColumns || '').split(',').filter(Boolean).length}</Descriptions.Item><Descriptions.Item label="调度">{values.scheduleMode || '-'}</Descriptions.Item></Descriptions>; }}</Form.Item></>}
      </ModalForm>
    </PageContainer>
  );
}
