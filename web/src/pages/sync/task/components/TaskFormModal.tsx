import { ModalForm, ProFormDependency, ProFormDigit, ProFormSelect, ProFormText } from '@ant-design/pro-components';
import { Alert, AutoComplete, Button, Descriptions, Divider, Form, message, Radio, Select, Space } from 'antd';
import { useEffect, useState } from 'react';
import type { DataSourceMetadataVO, DataSourceOptionVO } from '@/api/sync/data-source/types';
import type { SyncTaskForm, SyncTaskVO } from '@/api/sync/task/types';
import { createKafkaTopic, getDataSourceMetadata } from '@/api/sync/data-source';
import { addSyncTask, getSyncTask, updateSyncTask } from '@/api/sync/task';
import ColumnSelector from '@/components/sync/ColumnSelector';
import {
  jumpToMissing,
  SourceProbeResult,
  useCloseGuard,
  WizardFooter,
  WizardSteps
} from '@/components/sync/SyncWizard';
import { useSourceProbe, useTargetObjects } from '@/components/sync/useSourceProbe';
import {
  defaultForm,
  kafkaOutputFormatLabel,
  kafkaOutputFormatOptions,
  mappingRisks,
  reliableKeyOptions,
  sourceLabel,
  sourceTypeOf
} from '@/pages/sync/task/shared';
import { defaultKafkaTopic } from '@/utils/syncNaming';

const WIZARD_STEPS = ['数据源', '同步粒度', '目标端', '字段映射', '同步方式'];
/** Human label per field, used when sending the operator back to the step that is missing it. */
const FIELD_LABELS: Record<string, string> = {
  sourceId: '源数据源',
  targetId: '目标数据源',
  sourceTable: '源表',
  targetTable: '目标表',
  selectedColumns: '同步字段',
  syncKeyColumns: '同步键',
  taskName: '任务名称'
};
const LAST_STEP = WIZARD_STEPS.length - 1;
/** Fields each step must fill before the wizard advances. */
const REQUIRED_FIELDS_BY_STEP = [
  ['sourceId', 'targetId'],
  ['sourceTable'],
  ['targetSchema', 'targetTable', 'fullDataMode'],
  ['selectedColumns', 'syncKeyColumns'],
  []
];

export interface TaskFormModalProps {
  open: boolean;
  /** Set to edit that task, left out to create a new one. */
  task?: SyncTaskVO;
  dataSources: DataSourceOptionVO[];
  onClose: () => void;
  onSaved: () => void;
}

/**
 * The create / edit wizard. It owns the form and everything it introspects from the
 * source (databases, tables, column metadata, CDC precheck, target tables / Kafka topics),
 * so the list and the detail modal never share that state with it.
 */
export default function TaskFormModal({ open, task, dataSources, onClose, onSaved }: TaskFormModalProps) {
  const [form] = Form.useForm<SyncTaskForm>();
  const [wizardStep, setWizardStep] = useState(0);
  const [submitLoading, setSubmitLoading] = useState(false);
  // Connection test, CDC precheck, databases and tables of the source, with a live progress phase.
  const probe = useSourceProbe(dataSources);
  const { sourceTables, sourceDatabase, cdcPrecheck } = probe;
  const targets = useTargetObjects(dataSources);
  const targetTables = targets.names;
  const targetTablesLoading = targets.loading;
  const [topicMode, setTopicMode] = useState<'EXISTING' | 'CREATE'>('EXISTING');
  const [topicCreateLoading, setTopicCreateLoading] = useState(false);
  const [sourceMetadata, setSourceMetadata] = useState<DataSourceMetadataVO>();

  const taskId = task?.taskId;

  useEffect(() => {
    if (!open) return;
    setWizardStep(0);
    setDirty(false);
    resetMetadataState();
    resetTargetTables();
    form.resetFields();
    if (!taskId) {
      form.setFieldsValue(defaultForm);
      return;
    }
    void getSyncTask(taskId).then(res => {
      form.setFieldsValue({
        ...res.data,
        selectedColumns:
          typeof res.data.selectedColumns === 'string'
            ? res.data.selectedColumns.split(',').filter(Boolean)
            : res.data.selectedColumns
      });
      void loadSourceMetadata(res.data.sourceId, res.data.sourceTable);
      void loadTargetTables(res.data.targetId);
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, taskId]);

  function resetMetadataState() {
    probe.reset();
    setSourceMetadata(undefined);
  }

  function resetTargetTables() {
    targets.reset();
    setTopicMode('EXISTING');
  }

  const close = () => {
    resetMetadataState();
    resetTargetTables();
    onClose();
  };

  const { setDirty, confirmClose, modalProps } = useCloseGuard(close);

  const loadTargetTables = targets.load;

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

  /** Reconnects and re-introspects rather than trusting the stored connection state. */
  const loadSourceMetadata = async (sourceId?: string | number, tableName?: string) => {
    setSourceMetadata(undefined);
    await probe.probe(sourceId, async ({ sourceId: probedId, database, setPhase }) => {
      if (!tableName) return;
      setPhase('正在读取源表结构…');
      const table = tableName.includes('.') ? tableName.slice(tableName.lastIndexOf('.') + 1) : tableName;
      if (!table) return;
      const metadataResult = await getDataSourceMetadata(probedId, database, table);
      setSourceMetadata(metadataResult.data);
      if (!form.getFieldValue('selectedColumns'))
        form.setFieldValue(
          'selectedColumns',
          metadataResult.data.columns.map(column => column.name)
        );
      if (!form.getFieldValue('syncKeyColumns'))
        form.setFieldValue('syncKeyColumns', reliableKeyOptions(metadataResult.data)[0]?.value);
    });
  };

  const loadTableMetadata = async (tableName?: string) => {
    setSourceMetadata(undefined);
    const sourceId = form.getFieldValue('sourceId');
    if (!sourceId || !sourceDatabase || !tableName) return;
    await probe.track('正在读取源表结构…', async () => {
      const result = await getDataSourceMetadata(sourceId, sourceDatabase, tableName);
      setSourceMetadata(result.data);
      form.setFieldValue(
        'selectedColumns',
        result.data.columns.map(column => column.name)
      );
      form.setFieldValue('syncKeyColumns', reliableKeyOptions(result.data)[0]?.value);
      // Same-name is the overwhelmingly common case, so prefill it for every target type
      // (Kafka gets a sanitized topic name); the field stays editable.
      if (!form.getFieldValue('targetTable')) {
        const targetIsKafka = sourceTypeOf(dataSources, form.getFieldValue('targetId')) === 'KAFKA';
        form.setFieldValue('targetTable', targetIsKafka ? defaultKafkaTopic(sourceDatabase, tableName) : tableName);
      }
    });
  };

  /** Sends the operator to the step that owns {@code field} and highlights it there. */
  const jumpToField = (field: string) => {
    const step = REQUIRED_FIELDS_BY_STEP.findIndex(fields => fields.includes(field));
    jumpToMissing(form, WIZARD_STEPS, setWizardStep, { step, field, label: FIELD_LABELS[field] || field });
  };

  const nextWizardStep = async () => {
    const fields = REQUIRED_FIELDS_BY_STEP[wizardStep];
    if (fields?.length) {
      await form.validateFields(fields);
      if (wizardStep === 0 && !probe.connectionTest?.success) {
        message.error('请先选择可用的 MySQL 源数据源，并等待连接检查通过');
        return;
      }
      if (wizardStep === 1 && !sourceMetadata) {
        // Unreachable while a probe runs (the button is disabled then); this is the case where
        // the probe finished without metadata, i.e. no source table was picked.
        message.error('请先选择源表');
        return;
      }
    }
    setWizardStep(current => Math.min(current + 1, LAST_STEP));
  };

  const submitForm = async (values: SyncTaskForm) => {
    // Step panels unmount their controls; use the full form store so preserved
    // values from earlier steps are included in the final payload.
    const allValues = { ...form.getFieldsValue(true), ...values } as SyncTaskForm;
    const selectedColumns = Array.isArray(allValues.selectedColumns)
      ? allValues.selectedColumns
      : String(allValues.selectedColumns || '')
          .split(',')
          .filter(Boolean);
    // Name the step that is incomplete and take the operator there, instead of asking them to
    // walk back through five steps looking for it.
    const missing = (
      [
        ['sourceId', allValues.sourceId],
        ['targetId', allValues.targetId],
        ['sourceTable', allValues.sourceTable],
        ['targetTable', allValues.targetTable],
        ['selectedColumns', selectedColumns.length ? 'ok' : ''],
        ['syncKeyColumns', allValues.syncKeyColumns]
      ] as [string, unknown][]
    ).find(([, value]) => !value)?.[0];
    if (missing) {
      jumpToField(missing);
      return false;
    }
    if (allValues.syncMode === 'INCREMENTAL' && !cdcPrecheck?.passed) {
      setWizardStep(0);
      message.error('纯增量任务必须先通过 CDC 前置检查，已返回第 1 步「数据源」，请重新选择源数据源');
      return false;
    }
    const targetType = sourceTypeOf(dataSources, allValues.targetId);
    const payload = {
      ...allValues,
      selectedColumns: selectedColumns.join(','),
      targetSchema: targetType === 'POSTGRESQL' ? allValues.targetSchema : undefined,
      kafkaOutputFormat: targetType === 'KAFKA' ? allValues.kafkaOutputFormat : undefined
    };
    setSubmitLoading(true);
    try {
      if (allValues.taskId) await updateSyncTask(payload);
      else await addSyncTask(payload);
      message.success(allValues.taskId ? '任务保存成功' : '任务创建成功');
      close();
      onSaved();
      return true;
    } finally {
      setSubmitLoading(false);
    }
  };

  const sourceOptions = dataSources
    .filter(item => item.sourceType === 'MYSQL')
    .map(item => ({ label: sourceLabel(item), value: item.sourceId }));
  const targetOptions = dataSources
    .filter(item => item.sourceType === 'POSTGRESQL' || item.sourceType === 'MYSQL' || item.sourceType === 'KAFKA')
    .map(item => ({ label: sourceLabel(item), value: item.sourceId }));

  return (
    <ModalForm<SyncTaskForm>
      title={taskId ? '修改同步任务' : '新增同步任务'}
      open={open}
      form={form}
      preserve
      layout="vertical"
      width={760}
      modalProps={modalProps}
      onValuesChange={() => setDirty(true)}
      onFinish={submitForm}
      onFinishFailed={() => message.error('请先完善当前步骤的必填项')}
      submitter={{
        render: () => (
          <WizardFooter
            step={wizardStep}
            lastStep={LAST_STEP}
            busy={probe.loading}
            submitting={submitLoading}
            submitText={taskId ? '确认保存' : '确认创建'}
            onCancel={confirmClose}
            onPrev={() => setWizardStep(current => current - 1)}
            onNext={() => void nextWizardStep()}
            onSubmit={() => form.submit()}
          />
        )
      }}
    >
      <ProFormText name="taskId" hidden />
      <WizardSteps steps={WIZARD_STEPS} current={wizardStep} probePhase={probe.phase} />
      {wizardStep === 0 && (
        <>
          <Alert
            type="info"
            showIcon
            message="选择数据源后会重新连接并探查元数据，不直接信任历史连接状态。"
            style={{ marginBottom: 16 }}
          />
          <ProFormSelect
            name="sourceId"
            label="源数据源（MySQL）"
            options={sourceOptions}
            rules={[{ required: true, message: '请选择源数据源' }]}
            fieldProps={{
              loading: dataSources.length === 0,
              onChange: value => void loadSourceMetadata(value as string | number)
            }}
          />
          <SourceProbeResult probe={probe} databaseNote="当前单表任务使用数据源配置的默认数据库。" />
          <ProFormDependency name={['sourceId']}>
            {({ sourceId }) => (
              <ProFormSelect
                name="targetId"
                label="目标数据源（MySQL / PostgreSQL / Kafka）"
                options={targetOptions.filter(option => String(option.value) !== String(sourceId))}
                rules={[{ required: true, message: '请选择目标数据源' }]}
                fieldProps={{
                  onChange: value => {
                    form.setFieldValue('targetTable', undefined);
                    void loadTargetTables(value as string | number);
                  }
                }}
              />
            )}
          </ProFormDependency>
        </>
      )}
      {wizardStep === 1 && (
        <>
          <Alert
            type="info"
            showIcon
            message="当前入口创建单表任务；多表和整库同步请在“同步任务组”页面创建。"
            style={{ marginBottom: 16 }}
          />
          <ProFormSelect
            name="sourceTable"
            label="源表名"
            options={sourceTables.map(table => ({ label: table, value: table }))}
            showSearch
            rules={[{ required: true, message: '请选择源表' }]}
            fieldProps={{
              loading: probe.loading && sourceTables.length === 0,
              onChange: value => void loadTableMetadata(value as string)
            }}
          />
          {sourceMetadata && (
            <Alert
              type={
                sourceMetadata.primaryKeys.length > 0 || sourceMetadata.uniqueKeys.some(item => item.allNotNull)
                  ? 'success'
                  : 'warning'
              }
              showIcon
              message={`已读取 ${sourceMetadata.tableName} 元数据`}
              description={
                <Descriptions size="small" column={{ xs: 1, sm: 2 }}>
                  <Descriptions.Item label="字段数">{sourceMetadata.columns.length}</Descriptions.Item>
                  <Descriptions.Item label="字符集">{sourceMetadata.charset || '-'}</Descriptions.Item>
                  <Descriptions.Item label="主键">{sourceMetadata.primaryKeys.join(', ') || '无'}</Descriptions.Item>
                  <Descriptions.Item label="可靠唯一键">
                    {sourceMetadata.uniqueKeys
                      .filter(item => item.allNotNull)
                      .map(item => `${item.name} (${item.columns.join(', ')})`)
                      .join('; ') || '无'}
                  </Descriptions.Item>
                </Descriptions>
              }
            />
          )}
        </>
      )}
      {wizardStep === 2 && (
        <Form.Item noStyle shouldUpdate={(prev, current) => prev.targetId !== current.targetId}>
          {({ getFieldValue }) =>
            getFieldValue('targetId') && sourceTypeOf(dataSources, getFieldValue('targetId')) === 'KAFKA' ? (
              <>
                <Form.Item label="Kafka topic 来源">
                  <Radio.Group
                    value={topicMode}
                    onChange={event => setTopicMode(event.target.value)}
                    options={[
                      { label: '选择已有 topic', value: 'EXISTING' },
                      { label: '创建新 topic', value: 'CREATE' }
                    ]}
                  />
                </Form.Item>
                <Form.Item
                  name="targetTable"
                  preserve
                  label="目标 topic"
                  rules={[{ required: true, message: '请选择或填写目标 topic' }]}
                  extra={
                    topicMode === 'CREATE'
                      ? '平台将使用 1 个分区、1 个副本创建 topic，不会覆盖已存在的 topic。'
                      : '仅展示当前账号可见的 topic。'
                  }
                >
                  <AutoComplete
                    options={targetTables.map(table => ({ label: table, value: table }))}
                    allowClear
                    placeholder={
                      topicMode === 'CREATE'
                        ? '输入新 topic 名称'
                        : targetTablesLoading
                          ? '正在读取 topic 列表'
                          : '选择已有 topic'
                    }
                  />
                </Form.Item>
                {topicMode === 'CREATE' && (
                  <Button type="primary" loading={topicCreateLoading} onClick={() => void createTopic()}>
                    创建 topic
                  </Button>
                )}
                <Form.Item
                  name="kafkaOutputFormat"
                  preserve
                  label="Kafka 输出格式"
                  rules={[{ required: true, message: '请选择输出格式' }]}
                  extra="默认 JSON 是平台自带的事件格式；兼容格式可被对应生态工具或 SeaTunnel Kafka Source（同名 format）直接消费。消息 Key 始终为同步键 JSON，分区与顺序不变。"
                >
                  <Select options={kafkaOutputFormatOptions} placeholder="默认JSON" />
                </Form.Item>
                <Alert
                  type="info"
                  showIcon
                  message="选择已有 topic 或创建新 topic（默认 1 分区 1 副本）；创建 topic 需要 Kafka 凭证具备 Create 权限。"
                />
              </>
            ) : (
              <>
                <ProFormText name="targetSchema" label="目标 Schema（仅 PostgreSQL）" placeholder="默认 public" />
                <Form.Item
                  name="targetTable"
                  preserve
                  label="目标表名"
                  rules={[{ required: true, message: '请选择已有表或输入目标表名' }]}
                  extra="MySQL/PostgreSQL 可选择已有表或输入新表名。"
                >
                  <AutoComplete
                    options={targetTables.map(table => ({ label: table, value: table }))}
                    allowClear
                    placeholder={
                      targetTablesLoading ? '正在读取目标端表列表，可直接输入新表名' : '选择已有表或输入目标表名'
                    }
                  />
                </Form.Item>
                <Alert type="info" showIcon message="关系型目标已有表会执行兼容性检查；新表在任务启动时自动创建。" />
              </>
            )
          }
        </Form.Item>
      )}
      {wizardStep === 3 && sourceMetadata && (
        <>
          {mappingRisks(sourceMetadata).map(risk => (
            <Alert key={risk} type="warning" showIcon message={risk} style={{ marginBottom: 8 }} />
          ))}
          <Form.Item
            name="selectedColumns"
            label="纳入同步的字段"
            rules={[{ required: true, message: '至少选择一个字段' }]}
            extra="MVP 仅支持同名字段映射；同步键字段不可排除。"
          >
            <ColumnSelector
              columns={sourceMetadata.columns.map(column => ({ name: column.name, typeName: column.typeName }))}
              lockedColumns={String(form.getFieldValue('syncKeyColumns') || '')
                .split(',')
                .filter(Boolean)}
            />
          </Form.Item>
          <Form.Item
            name="syncKeyColumns"
            label="同步键"
            rules={[{ required: true, message: '请选择可靠同步键' }]}
            extra="优先使用主键；没有主键时仅可选择所有字段均为非空的唯一键。"
          >
            <Select
              options={reliableKeyOptions(sourceMetadata)}
              disabled={reliableKeyOptions(sourceMetadata).length === 0}
              placeholder={reliableKeyOptions(sourceMetadata).length ? '请选择同步键' : '源表没有可靠同步键'}
            />
          </Form.Item>
          {reliableKeyOptions(sourceMetadata).length === 0 && (
            <Alert
              type="warning"
              showIcon
              message="该表没有可靠同步键，只能创建全量任务；增量相关模式在保存时会被阻断。"
            />
          )}
        </>
      )}
      {wizardStep === 4 && (
        <>
          <ProFormText name="taskName" label="任务名称" rules={[{ required: true, message: '请输入任务名称' }]} />
          <ProFormSelect
            name="syncMode"
            label="同步模式"
            options={[
              { label: '全量同步', value: 'FULL' },
              { label: '纯增量', value: 'INCREMENTAL' },
              { label: '全量 + CDC', value: 'FULL_CDC' }
            ]}
            rules={[{ required: true }]}
          />
          <Form.Item
            noStyle
            shouldUpdate={(prev, current) =>
              prev.syncMode !== current.syncMode || prev.incrementalStartupMode !== current.incrementalStartupMode
            }
          >
            {({ getFieldValue }) =>
              getFieldValue('syncMode') === 'INCREMENTAL' ? (
                <>
                  <Alert
                    type="warning"
                    showIcon
                    message="纯增量不会补齐任务创建前的历史数据，目标端必须已有可信基线。"
                    style={{ marginBottom: 12 }}
                  />
                  {cdcPrecheck && !cdcPrecheck.passed && (
                    <Alert
                      type="error"
                      showIcon
                      message="CDC 前置检查未通过，不能创建纯增量任务。"
                      description={cdcPrecheck.message}
                      style={{ marginBottom: 12 }}
                    />
                  )}
                  <ProFormSelect
                    name="incrementalStartupMode"
                    label="增量启动位点"
                    options={[
                      { label: '从创建后的最新位点开始', value: 'LATEST' },
                      { label: '按指定时间开始', value: 'TIMESTAMP' },
                      { label: '指定 binlog 文件和位置', value: 'SPECIFIC' }
                    ]}
                    rules={[{ required: true }]}
                  />
                  {getFieldValue('incrementalStartupMode') === 'TIMESTAMP' && (
                    <ProFormText
                      name="incrementalStartupTimestamp"
                      label="启动时间"
                      placeholder="例如 2026-08-27T12:30:00"
                      fieldProps={{ type: 'datetime-local' }}
                      rules={[{ required: true, message: '请选择启动时间' }]}
                      extra="按源端 Asia/Shanghai 时区换算为 SeaTunnel 毫秒时间戳。"
                    />
                  )}
                  {getFieldValue('incrementalStartupMode') === 'SPECIFIC' && (
                    <Space align="start">
                      <ProFormText
                        name="incrementalStartupBinlogFile"
                        label="binlog 文件"
                        placeholder="mysql-bin.000001"
                        rules={[{ required: true, message: '请输入 binlog 文件' }]}
                        fieldProps={{ style: { width: 250 } }}
                      />
                      <ProFormDigit
                        name="incrementalStartupBinlogPosition"
                        label="binlog 位置"
                        min={4}
                        rules={[{ required: true, message: '请输入 binlog 位置' }]}
                        fieldProps={{ style: { width: 180 } }}
                      />
                    </Space>
                  )}
                </>
              ) : null
            }
          </Form.Item>
          <ProFormSelect
            name="ddlPolicy"
            label="DDL 策略"
            options={[
              { label: '失败即停', value: 'FAIL' },
              { label: '忽略', value: 'IGNORE' }
            ]}
            rules={[{ required: true }]}
          />
          <ProFormSelect
            name="scheduleMode"
            label="调度模式"
            options={[
              { label: '保存后执行一次', value: 'ONCE' },
              { label: 'Cron 定时执行', value: 'CRON' },
              { label: '常驻实时（手动启动）', value: 'REALTIME' },
              { label: '手动（不自动触发）', value: 'MANUAL' }
            ]}
            rules={[{ required: true }]}
          />
          <Form.Item noStyle shouldUpdate={(prev, current) => prev.scheduleMode !== current.scheduleMode}>
            {({ getFieldValue }) =>
              getFieldValue('scheduleMode') === 'CRON' ? (
                <ProFormText
                  name="cronExpression"
                  label="Cron 表达式"
                  placeholder="例如 0 0 2 * * ?（秒 分 时 日 月 周）"
                  rules={[{ required: true, message: '请输入 Cron 表达式' }]}
                  extra="使用 Spring 6 位 Cron，保存时会校验并计算下次执行时间。"
                />
              ) : null
            }
          </Form.Item>
          <Form.Item
            noStyle
            shouldUpdate={(prev, current) =>
              prev.fullDataMode !== current.fullDataMode || prev.syncMode !== current.syncMode
            }
          >
            {({ getFieldValue }) =>
              getFieldValue('fullDataMode') === 'OVERWRITE' && getFieldValue('syncMode') === 'FULL_CDC' ? (
                <Alert
                  type="warning"
                  showIcon
                  message="全量 + CDC 的覆盖刷新需要一致性切换水位，当前 MVP 请使用合并（upsert）或创建纯全量覆盖任务。"
                />
              ) : getFieldValue('fullDataMode') === 'OVERWRITE' && getFieldValue('syncMode') !== 'INCREMENTAL' ? (
                <Alert type="warning" showIcon message="覆盖刷新将先写入临时表，作业成功后才替换正式表。" />
              ) : null
            }
          </Form.Item>
          <Divider titlePlacement="left" plain>
            源库保护
          </Divider>
          <Space wrap align="start">
            <ProFormDigit
              name="readLimitRowsPerSecond"
              label="最大行数/秒"
              min={1}
              max={100000}
              rules={[{ required: true }]}
              fieldProps={{ style: { width: 180 } }}
            />
            <ProFormDigit
              name="readLimitBytesPerSecond"
              label="最大字节/秒"
              min={1}
              max={1073741824}
              rules={[{ required: true }]}
              fieldProps={{ style: { width: 210 } }}
            />
            <ProFormDigit
              name="snapshotParallelism"
              label="快照并行度"
              min={1}
              max={4}
              rules={[{ required: true }]}
              fieldProps={{ style: { width: 150 } }}
            />
            <ProFormDigit
              name="sourceConnectionLimit"
              label="CDC 连接池上限"
              min={1}
              max={8}
              rules={[{ required: true }]}
              fieldProps={{ style: { width: 180 } }}
            />
          </Space>
          <Divider titlePlacement="left" plain>
            提交确认
          </Divider>
          <Form.Item noStyle shouldUpdate>
            {({ getFieldsValue }) => {
              const values = getFieldsValue(true);
              const targetType = sourceTypeOf(dataSources, values.targetId);
              return (
                <Descriptions size="small" column={2} bordered>
                  <Descriptions.Item label="源端">
                    {sourceDatabase || '-'} / {values.sourceTable || '-'}
                  </Descriptions.Item>
                  <Descriptions.Item label="目标端">
                    {targetType === 'POSTGRESQL' ? `${values.targetSchema || 'public'} / ` : ''}
                    {values.targetTable || '-'}
                  </Descriptions.Item>
                  <Descriptions.Item label="同步模式">{values.syncMode || '-'}</Descriptions.Item>
                  {targetType === 'KAFKA' && (
                    <Descriptions.Item label="输出格式">
                      {kafkaOutputFormatLabel(values.kafkaOutputFormat)}
                    </Descriptions.Item>
                  )}
                  <Descriptions.Item label="同步键">{values.syncKeyColumns || '-'}</Descriptions.Item>
                  <Descriptions.Item label="字段数">
                    {Array.isArray(values.selectedColumns)
                      ? values.selectedColumns.length
                      : String(values.selectedColumns || '')
                          .split(',')
                          .filter(Boolean).length}
                  </Descriptions.Item>
                  <Descriptions.Item label="调度">{values.scheduleMode || '-'}</Descriptions.Item>
                </Descriptions>
              );
            }}
          </Form.Item>
        </>
      )}
    </ModalForm>
  );
}
