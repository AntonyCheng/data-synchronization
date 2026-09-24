import type { FormListOperation } from 'antd';
import { ModalForm, ProFormDigit, ProFormSelect, ProFormText } from '@ant-design/pro-components';
import {
  Alert,
  AutoComplete,
  Button,
  Collapse,
  Descriptions,
  Divider,
  Form,
  Input,
  message,
  Radio,
  Select,
  Space,
  Switch,
  Tag
} from 'antd';
import { useEffect, useState } from 'react';
import type { DataSourceMetadataVO, DataSourceOptionVO } from '@/api/sync/data-source/types';
import type { SyncTaskGroupForm, SyncTaskGroupItemForm, SyncTaskGroupVO } from '@/api/sync/group/types';
import { createKafkaTopic, getDataSourceMetadata } from '@/api/sync/data-source';
import { addSyncTaskGroup, getSyncTaskGroup, getSyncTaskGroupLimits, updateSyncTaskGroup } from '@/api/sync/group';
import ColumnSelector from '@/components/sync/ColumnSelector';
import {
  jumpToMissing,
  type MissingField,
  SourceProbeResult,
  useCloseGuard,
  WizardFooter,
  WizardSteps
} from '@/components/sync/SyncWizard';
import { useSourceProbe, useTargetObjects } from '@/components/sync/useSourceProbe';
import { emptyForm, kafkaOutputFormatLabel, kafkaOutputFormatOptions, sourceTypeOf } from '@/pages/sync/group/shared';
import { syncKeyOptions } from '@/utils/syncKeys';
import { defaultTargetName } from '@/utils/syncNaming';
import { columnList, fractionalTimeColumns, timePrecisionWarning } from '@/utils/syncTypeRisks';

const WIZARD_STEPS = ['数据源', '表与字段', '同步方式与限速'];
const LAST_STEP = WIZARD_STEPS.length - 1;
/** The backend default of `sync.group.max-tables`, used until GET /sync/group/limits answers. */
const DEFAULT_MAX_TABLES = 20;

type GroupField = keyof SyncTaskGroupForm;
type ItemField = 'sourceTable' | 'targetTable' | 'selectedColumns' | 'syncKeyColumns';

/** Human label per field, used when sending the operator back to the step that is missing it. */
const FIELD_LABELS: Partial<Record<GroupField, string>> = {
  sourceId: '源数据源',
  targetId: '目标数据源',
  kafkaOutputFormat: 'Kafka 输出格式',
  syncScope: '同步粒度',
  groupName: '任务组名称',
  syncMode: '同步模式',
  readLimitRowsPerSecond: '最大行数/秒',
  readLimitBytesPerSecond: '最大字节/秒',
  snapshotParallelism: '快照并行度',
  sourceConnectionLimit: 'CDC 连接池上限'
};
/** Group-level fields each step must fill; the table items of step 2 are checked separately. */
const REQUIRED_FIELDS_BY_STEP: GroupField[][] = [
  ['sourceId', 'targetId', 'kafkaOutputFormat'],
  ['syncScope'],
  [
    'groupName',
    'syncMode',
    'readLimitRowsPerSecond',
    'readLimitBytesPerSecond',
    'snapshotParallelism',
    'sourceConnectionLimit'
  ]
];
const ITEM_FIELD_LABELS: Record<ItemField, string> = {
  sourceTable: '源表',
  targetTable: '目标表',
  selectedColumns: '同步字段',
  syncKeyColumns: '同步键'
};
const SYNC_MODE_LABELS: Record<string, string> = { FULL: '全量同步', INCREMENTAL: '纯增量', FULL_CDC: '全量 + CDC' };

interface GroupMissingField extends MissingField {
  /** Set when the gap is inside one table item, so its panel can be opened. */
  itemIndex?: number;
}

const isBlank = (value: unknown) =>
  value === undefined || value === null || value === '' || (Array.isArray(value) && value.length === 0);

/** Metadata is cached per source + table: items are unique by table, but positions shift on delete. */
const metadataKey = (sourceId: string | number | undefined, table: string) => `${sourceId}:${table}`;

/**
 * The first required value that is still empty, in the order the wizard asks for it. Steps
 * unmount their controls, so this reads the whole store - and it also covers a table's column
 * selection and sync key, which only render once that table's structure has been read.
 */
function findMissing(values: SyncTaskGroupForm, kafkaTarget: boolean): GroupMissingField | undefined {
  const missingIn = (step: number) =>
    REQUIRED_FIELDS_BY_STEP[step]
      .filter(field => field !== 'kafkaOutputFormat' || kafkaTarget)
      .find(field => isBlank(values[field]));
  for (const step of [0, 1]) {
    const field = missingIn(step);
    if (field) return { step, field, label: FIELD_LABELS[field] || field };
  }
  if (values.syncScope !== 'DATABASE') {
    const items = values.items || [];
    if (!items.length) return { step: 1, field: 'items', label: '同步表（请至少添加一张表）' };
    for (let index = 0; index < items.length; index++) {
      const item = items[index] || {};
      const field = (Object.keys(ITEM_FIELD_LABELS) as ItemField[]).find(key => isBlank(item[key]));
      if (field) {
        const fieldLabel = field === 'targetTable' && kafkaTarget ? '目标 topic' : ITEM_FIELD_LABELS[field];
        return {
          step: 1,
          field: ['items', index, field],
          label: `表 ${item.sourceTable || `#${index + 1}`} 的${fieldLabel}`,
          itemIndex: index
        };
      }
    }
  }
  const field = missingIn(2);
  return field ? { step: 2, field, label: FIELD_LABELS[field] || field } : undefined;
}

export interface GroupFormModalProps {
  open: boolean;
  /** Set to edit that group, left out to create a new one. */
  group?: SyncTaskGroupVO;
  dataSources: DataSourceOptionVO[];
  onClose: () => void;
  onSaved: () => void;
}

/**
 * Create / edit wizard for a task group, in the same shape as the single-table wizard:
 * 1. 数据源 - source (probed: connection, CDC precheck, databases, tables) and target, plus the
 *    Kafka output format; 2. 表与字段 - multi-table vs whole-database, and for multi-table the
 *    table items with their target, columns and sync key; 3. 同步方式与限速 - name, sync mode,
 *    per-item source protection and a summary. It owns the form and all introspected metadata.
 */
export default function GroupFormModal({ open, group, dataSources, onClose, onSaved }: GroupFormModalProps) {
  const [form] = Form.useForm<SyncTaskGroupForm>();
  const [wizardStep, setWizardStep] = useState(0);
  const [submitLoading, setSubmitLoading] = useState(false);
  const probe = useSourceProbe(dataSources);
  const targets = useTargetObjects(dataSources);
  const [topicCreateLoading, setTopicCreateLoading] = useState<number>();
  const [tableMetadata, setTableMetadata] = useState<Record<string, DataSourceMetadataVO>>({});
  const [failedTables, setFailedTables] = useState<string[]>([]);
  // Table structures being read right now; they load in parallel after a batch add.
  const [tableLoads, setTableLoads] = useState(0);
  // Which table panels are open. A long single-page form was the complaint: by the third table
  // you are scrolling past everything you already configured.
  const [openItems, setOpenItems] = useState<string[]>([]);
  const [tablesToAdd, setTablesToAdd] = useState<string[]>([]);
  const [bulkSchema, setBulkSchema] = useState('');
  const { setDirty, confirmClose, modalProps } = useCloseGuard(onClose);
  // The table cap is a server setting (sync.group.max-tables); the wizard only mirrors it.
  const [maxTables, setMaxTables] = useState(DEFAULT_MAX_TABLES);
  useEffect(() => {
    if (!open) return;
    getSyncTaskGroupLimits()
      .then(res => {
        if (res.data?.maxTables) setMaxTables(res.data.maxTables);
      })
      .catch(() => undefined);
  }, [open]);

  // preserve: each step unmounts the others' controls, and a plain useWatch then reads undefined.
  const syncScope = Form.useWatch('syncScope', { form, preserve: true }) || 'MULTI_TABLE';
  const itemValues: SyncTaskGroupItemForm[] = Form.useWatch('items', { form, preserve: true }) || [];
  const sourceId = Form.useWatch('sourceId', { form, preserve: true });
  const targetId = Form.useWatch('targetId', { form, preserve: true });
  const targetType = sourceTypeOf(dataSources, targetId);
  const kafkaTarget = targetType === 'KAFKA';
  const postgresTarget = targetType === 'POSTGRESQL';
  const groupId = group?.groupId;
  const busy = probe.loading || tableLoads > 0;
  const busyPhase = probe.phase || (tableLoads > 0 ? `正在读取 ${tableLoads} 张表的结构…` : undefined);

  useEffect(() => {
    if (!open) return;
    setWizardStep(0);
    setDirty(false);
    probe.reset();
    targets.reset();
    setTableMetadata({});
    setFailedTables([]);
    setOpenItems([]);
    setTablesToAdd([]);
    setBulkSchema('');
    form.resetFields();
    if (!groupId) {
      form.setFieldsValue(emptyForm);
      return;
    }
    void getSyncTaskGroup(groupId).then(result => {
      const items = (result.data.items || []).map(item => ({
        ...item,
        selectedColumns: columnList(item.selectedColumns)
      }));
      form.setFieldsValue({ ...result.data, items });
      void targets.load(result.data.targetId);
      void probe.probe(result.data.sourceId);
      items.forEach(item => void loadTableMetadata(item.sourceTable, result.data.sourceId, item.sourceDatabase));
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, groupId]);

  const sourceOf = (id?: string | number) => dataSources.find(item => String(item.sourceId) === String(id));

  /** The database multi-table items are picked from: the data source's own, else the first probed. */
  const sourceDatabaseName = () => sourceOf(form.getFieldValue('sourceId'))?.databaseName || probe.sourceDatabase;

  /**
   * Reads one table's columns and keys, then fills in that item's column selection and sync key
   * if they are still empty (an edited item keeps what was saved).
   */
  const loadTableMetadata = async (table?: string, fromSourceId?: string | number, database?: string) => {
    const source = sourceOf(fromSourceId ?? form.getFieldValue('sourceId'));
    if (!source || !table) return;
    const key = metadataKey(source.sourceId, table);
    setFailedTables(current => current.filter(item => item !== key));
    setTableLoads(count => count + 1);
    try {
      const result = await getDataSourceMetadata(
        source.sourceId,
        database || source.databaseName || probe.sourceDatabase,
        table
      );
      setTableMetadata(current => ({ ...current, [key]: result.data }));
      // The operator may have switched source or dropped the table while this was in flight.
      if (String(form.getFieldValue('sourceId')) !== String(source.sourceId)) return;
      const items: SyncTaskGroupItemForm[] = form.getFieldValue('items') || [];
      const index = items.findIndex(item => item?.sourceTable === table);
      if (index < 0) return;
      if (isBlank(items[index].selectedColumns))
        form.setFieldValue(
          ['items', index, 'selectedColumns'],
          result.data.columns.map(column => column.name)
        );
      if (isBlank(items[index].syncKeyColumns))
        form.setFieldValue(['items', index, 'syncKeyColumns'], syncKeyOptions(result.data)[0]?.value);
    } catch {
      // request() has already shown the error; the panel offers a retry.
      setFailedTables(current => [...current, key]);
    } finally {
      setTableLoads(count => count - 1);
    }
  };

  /** Tables, columns and keys all belong to the source, so a different source starts the list over. */
  const changeSource = (value?: string | number) => {
    const items: SyncTaskGroupItemForm[] = form.getFieldValue('items') || [];
    if (items.length) {
      form.setFieldValue('items', []);
      setOpenItems([]);
      message.warning(`已更换源数据源，原来的 ${items.length} 张表已移除，请在第 2 步重新添加`);
    }
    form.setFieldValue('sourceDatabase', undefined);
    // A MySQL target can also be picked as the source; the same data source cannot be both.
    if (String(form.getFieldValue('targetId')) === String(value)) {
      form.setFieldValue('targetId', undefined);
      targets.reset();
    }
    setTablesToAdd([]);
    void probe.probe(value);
  };

  const changeTarget = (value?: string | number) => {
    const nextType = sourceTypeOf(dataSources, value);
    const wasKafka = kafkaTarget;
    const nowKafka = nextType === 'KAFKA';
    const database = sourceDatabaseName();
    const items: SyncTaskGroupItemForm[] = form.getFieldValue('items') || [];
    form.setFieldsValue({
      items: items.map(item => ({
        ...item,
        targetSchema: nextType === 'POSTGRESQL' ? item.targetSchema || 'public' : undefined,
        // A name still at the old target type's default follows the new type; a typed one is kept.
        targetTable:
          wasKafka !== nowKafka &&
          item.sourceTable &&
          item.targetTable === defaultTargetName(database, item.sourceTable, wasKafka)
            ? defaultTargetName(database, item.sourceTable, nowKafka)
            : item.targetTable
      }))
    });
    if (nowKafka && !form.getFieldValue('kafkaOutputFormat')) form.setFieldValue('kafkaOutputFormat', 'ENVELOPE');
    void targets.load(value);
  };

  /** Re-points one item at another table: its column selection and key belonged to the old one. */
  const changeItemTable = (index: number, table: string, previousTable?: string) => {
    const items: SyncTaskGroupItemForm[] = [...(form.getFieldValue('items') || [])];
    const item = items[index] || {};
    const database = sourceDatabaseName();
    const followsDefault =
      !item.targetTable ||
      (previousTable !== undefined && item.targetTable === defaultTargetName(database, previousTable, kafkaTarget));
    items[index] = {
      ...item,
      sourceTable: table,
      selectedColumns: undefined,
      syncKeyColumns: undefined,
      targetTable: followsDefault ? defaultTargetName(database, table, kafkaTarget) : item.targetTable
    };
    form.setFieldsValue({ items });
    void loadTableMetadata(table);
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
      await targets.load(targetId);
      message.success(`topic ${result.data.topic} 创建成功`);
    } finally {
      setTopicCreateLoading(undefined);
    }
  };

  /** Takes the operator to the step (and table panel) that is missing something. */
  const goToMissing = (missing: GroupMissingField) => {
    if (missing.itemIndex !== undefined) {
      const key = String(missing.itemIndex);
      setOpenItems(current => (current.includes(key) ? current : [...current, key]));
    }
    jumpToMissing(form, WIZARD_STEPS, setWizardStep, missing);
  };

  const tooManyTables = (values: SyncTaskGroupForm) =>
    values.syncScope !== 'DATABASE' && (values.items?.length || 0) > maxTables;

  const nextWizardStep = async () => {
    try {
      // Only the current step's controls are mounted, so this checks exactly this step.
      await form.validateFields();
    } catch (error) {
      const errorFields = (error as { errorFields?: { name: (string | number)[] }[] }).errorFields || [];
      const failedItems = errorFields.filter(field => field.name[0] === 'items').map(field => String(field.name[1]));
      if (failedItems.length) {
        // An error inside a collapsed panel would otherwise be invisible.
        setOpenItems(current => [...new Set([...current, ...failedItems])]);
        message.error('请检查标红的表项');
      }
      return;
    }
    const values = form.getFieldsValue(true) as SyncTaskGroupForm;
    const missing = findMissing(values, kafkaTarget);
    if (missing && missing.step <= wizardStep) {
      if (missing.field === 'items' && missing.step === wizardStep) message.error('请至少添加一张表');
      else goToMissing(missing);
      return;
    }
    if (wizardStep === 0 && !probe.connectionTest?.success) {
      message.error('请先选择可用的 MySQL 源数据源，并等待连接检查通过');
      return;
    }
    if (wizardStep === 1 && tooManyTables(values)) {
      message.error(`单个任务组最多 ${maxTables} 张表，当前已选 ${values.items?.length} 张，请删除多余的表`);
      return;
    }
    setWizardStep(current => Math.min(current + 1, LAST_STEP));
  };

  const submitForm = async (values: SyncTaskGroupForm) => {
    // Step panels unmount their controls; use the full form store so values from earlier
    // steps are included in the payload.
    const allValues = { ...form.getFieldsValue(true), ...values } as SyncTaskGroupForm;
    const kafka = sourceTypeOf(dataSources, allValues.targetId) === 'KAFKA';
    const missing = findMissing(allValues, kafka);
    if (missing) {
      goToMissing(missing);
      return false;
    }
    if (tooManyTables(allValues)) {
      setWizardStep(1);
      message.error(`单个任务组最多 ${maxTables} 张表，已返回第 2 步「表与字段」，请删除多余的表`);
      return false;
    }
    if (allValues.syncMode === 'INCREMENTAL' && !probe.cdcPrecheck?.passed) {
      setWizardStep(0);
      message.error('纯增量任务组必须先通过 CDC 前置检查，已返回第 1 步「数据源」，请重新选择源数据源');
      return false;
    }
    const databaseScope = allValues.syncScope === 'DATABASE';
    const payload: SyncTaskGroupForm = {
      ...allValues,
      kafkaOutputFormat: kafka ? allValues.kafkaOutputFormat : undefined,
      // A multi-table draft's items stay in the store after switching to whole-database; those
      // groups discover their items server-side and must not submit them.
      items: databaseScope
        ? []
        : (allValues.items || []).map(item => ({
            ...item,
            selectedColumns: columnList(item.selectedColumns).join(',')
          }))
    };
    setSubmitLoading(true);
    try {
      if (allValues.groupId) await updateSyncTaskGroup(payload);
      else await addSyncTaskGroup(payload);
      message.success(allValues.groupId ? '任务组保存成功' : '任务组创建成功');
      onClose();
      onSaved();
      return true;
    } finally {
      setSubmitLoading(false);
    }
  };

  const optionLabel = (item: DataSourceOptionVO) => `${item.sourceName} (${item.databaseName || item.sourceType})`;
  const sourceOptions = dataSources
    .filter(item => item.sourceType === 'MYSQL')
    .map(item => ({ label: optionLabel(item), value: item.sourceId }));
  const targetOptions = dataSources
    .filter(item => item.sourceType === 'POSTGRESQL' || item.sourceType === 'MYSQL' || item.sourceType === 'KAFKA')
    .filter(item => String(item.sourceId) !== String(sourceId))
    .map(item => ({ label: optionLabel(item), value: item.sourceId }));

  const renderItems = () => (
    <Form.List name="items">
      {(fields, { add, remove }) => {
        const chosen = new Set(itemValues.map(item => item?.sourceTable).filter(Boolean));
        const addable = probe.sourceTables.filter(table => !chosen.has(table));

        /** Adds one panel per picked table, pre-filled the way a single table would be. */
        const addPicked = (addItem: FormListOperation['add']) => {
          const database = sourceDatabaseName();
          tablesToAdd.forEach(table =>
            addItem({
              sourceTable: table,
              targetTable: defaultTargetName(database, table, kafkaTarget),
              targetSchema: postgresTarget ? bulkSchema.trim() || 'public' : undefined
            })
          );
          tablesToAdd.forEach(table => void loadTableMetadata(table));
          setOpenItems(current => [...current, ...tablesToAdd.map((_, offset) => String(fields.length + offset))]);
          setTablesToAdd([]);
          setDirty(true);
        };

        const removeItem = (index: number) => {
          remove(index);
          // Panels are keyed by position, so the ones after the removed item move up by one.
          setOpenItems(current =>
            current
              .filter(key => key !== String(index))
              .map(key => (Number(key) > index ? String(Number(key) - 1) : key))
          );
        };

        const applySchemaToAll = () => {
          const schema = bulkSchema.trim() || 'public';
          const items: SyncTaskGroupItemForm[] = form.getFieldValue('items') || [];
          form.setFieldsValue({ items: items.map(item => ({ ...item, targetSchema: schema })) });
          setDirty(true);
          message.success(`已将目标 Schema 设为 ${schema}`);
        };

        return (
          <Space orientation="vertical" style={{ width: '100%' }} size={8}>
            {kafkaTarget && (
              <Alert
                type="info"
                showIcon
                title="每张表写入一个 topic：可选择已有 topic，也可输入名称后点「创建 topic」；平台不会依赖 broker 自动建 topic。"
              />
            )}
            <Space wrap style={{ width: '100%', justifyContent: 'space-between' }}>
              <Space wrap size={4}>
                <Select
                  mode="multiple"
                  allowClear
                  showSearch
                  aria-label="选择要加入的源表"
                  value={tablesToAdd}
                  onChange={setTablesToAdd}
                  options={addable.map(table => ({ label: table, value: table }))}
                  placeholder={probe.sourceTables.length ? '选择要加入的源表（可多选）' : '源库中没有读到可选的表'}
                  style={{ minWidth: 320 }}
                  maxTagCount="responsive"
                />
                <Button type="primary" disabled={tablesToAdd.length === 0} onClick={() => addPicked(add)}>
                  {tablesToAdd.length ? `添加 ${tablesToAdd.length} 张表` : '添加表'}
                </Button>
                {postgresTarget && (
                  <>
                    <Input
                      value={bulkSchema}
                      onChange={event => setBulkSchema(event.target.value)}
                      placeholder="目标 Schema"
                      style={{ width: 140 }}
                    />
                    <Button disabled={fields.length === 0} onClick={applySchemaToAll}>
                      应用到全部表项
                    </Button>
                  </>
                )}
              </Space>
              <Tag color={fields.length > maxTables ? 'error' : fields.length ? 'processing' : 'default'}>
                已选 {fields.length} / 上限 {maxTables} 张
              </Tag>
            </Space>
            {fields.length === 0 && (
              <Alert type="info" showIcon title="还没有表项：在上面挑选源表后点「添加」，可一次加入多张。" />
            )}
            <Collapse
              activeKey={openItems}
              onChange={keys => setOpenItems(([] as (string | number)[]).concat(keys).map(String))}
              items={fields.map(({ key, ...field }) => {
                const item = itemValues[field.name] || {};
                const tableKey = item.sourceTable ? metadataKey(sourceId, item.sourceTable) : '';
                const metadata = tableKey ? tableMetadata[tableKey] : undefined;
                const failed = tableKey !== '' && failedTables.includes(tableKey);
                const columnCount = columnList(item.selectedColumns).length;
                const topicMissing =
                  kafkaTarget &&
                  !targets.loading &&
                  !!item.targetTable &&
                  !targets.names.includes(String(item.targetTable).trim());
                return {
                  key: String(field.name),
                  // Kept mounted so validation still reaches fields in a collapsed panel.
                  forceRender: true,
                  label: (
                    <Space wrap size={8}>
                      <span>{item.sourceTable || '未选择源表'}</span>
                      <span style={{ opacity: 0.45 }}>→</span>
                      <span>{item.targetTable || (kafkaTarget ? '未填 topic' : '未填目标表')}</span>
                      {columnCount > 0 && <Tag>已选 {columnCount} 字段</Tag>}
                      {item.syncKeyColumns && <Tag color="blue">键 {item.syncKeyColumns}</Tag>}
                      {failed && <Tag color="error">结构读取失败</Tag>}
                    </Space>
                  ),
                  extra: (
                    <Button
                      danger
                      type="text"
                      size="small"
                      onClick={event => {
                        event.stopPropagation();
                        removeItem(field.name);
                      }}
                    >
                      删除
                    </Button>
                  ),
                  children: (
                    <>
                      <Space align="start" wrap>
                        <Form.Item {...field} name={[field.name, 'sourceDatabase']} hidden>
                          <Input />
                        </Form.Item>
                        <Form.Item
                          {...field}
                          name={[field.name, 'sourceTable']}
                          label="源表"
                          rules={[
                            { required: true, message: '请选择源表' },
                            {
                              validator: (_rule, value) => {
                                if (!value) return Promise.resolve();
                                const items: SyncTaskGroupItemForm[] = form.getFieldValue('items') || [];
                                const duplicate = items.some(
                                  (other, index) => index !== field.name && other?.sourceTable === value
                                );
                                return duplicate
                                  ? Promise.reject(new Error('该表已在本任务组中选过'))
                                  : Promise.resolve();
                              }
                            }
                          ]}
                        >
                          <Select
                            showSearch
                            options={probe.sourceTables.map(table => ({ label: table, value: table }))}
                            style={{ width: 220 }}
                            onChange={value => changeItemTable(field.name, value, item.sourceTable)}
                          />
                        </Form.Item>
                        {postgresTarget && (
                          <Form.Item {...field} name={[field.name, 'targetSchema']} label="目标 Schema">
                            <Input placeholder="public" style={{ width: 120 }} />
                          </Form.Item>
                        )}
                        <Form.Item
                          {...field}
                          name={[field.name, 'targetTable']}
                          label={kafkaTarget ? '目标 topic' : '目标表'}
                          rules={[{ required: true, message: kafkaTarget ? '请选择或填写目标 topic' : '请输入目标表' }]}
                          extra={topicMissing ? '该 topic 尚不存在，请点「创建 topic」' : undefined}
                          style={{ maxWidth: 260 }}
                        >
                          <AutoComplete
                            options={targets.names.map(table => ({ label: table, value: table }))}
                            allowClear
                            placeholder={
                              kafkaTarget
                                ? targets.loading
                                  ? '正在读取 topic 列表'
                                  : '选择已有 topic 或输入新 topic'
                                : targets.loading
                                  ? '正在读取目标表，可直接输入新表名'
                                  : '选择已有表或输入新表名'
                            }
                            style={{ width: 220 }}
                          />
                        </Form.Item>
                        {kafkaTarget && (
                          <Form.Item label=" " colon={false}>
                            <Button
                              loading={topicCreateLoading === field.name}
                              onClick={() => void createGroupTopic(field.name)}
                            >
                              创建 topic
                            </Button>
                          </Form.Item>
                        )}
                      </Space>
                      {metadata ? (
                        <Space orientation="vertical" size={4} style={{ width: '100%' }}>
                          <Form.Item
                            {...field}
                            name={[field.name, 'selectedColumns']}
                            label="同步字段"
                            rules={[{ required: true, message: '至少选择一个同步字段' }]}
                            extra="同步键字段不可排除。"
                          >
                            <ColumnSelector
                              columns={metadata.columns.map(column => ({
                                name: column.name,
                                typeName: column.typeName
                              }))}
                              lockedColumns={String(item.syncKeyColumns || '')
                                .split(',')
                                .filter(Boolean)}
                            />
                          </Form.Item>
                          <Form.Item
                            {...field}
                            name={[field.name, 'syncKeyColumns']}
                            label="同步键"
                            rules={[{ required: true, message: '请选择可靠同步键' }]}
                          >
                            <Select
                              options={syncKeyOptions(metadata)}
                              disabled={syncKeyOptions(metadata).length === 0}
                              placeholder="请选择主键或非空唯一键"
                            />
                          </Form.Item>
                          {syncKeyOptions(metadata).length === 0 && (
                            <Alert type="warning" showIcon title="该表没有可靠同步键，无法加入 CDC 任务组。" />
                          )}
                        </Space>
                      ) : failed ? (
                        <Alert
                          type="warning"
                          showIcon
                          title="未能读取该表的字段结构，暂时无法选择同步字段和同步键。"
                          action={
                            <Button
                              size="small"
                              onClick={() => void loadTableMetadata(item.sourceTable, undefined, item.sourceDatabase)}
                            >
                              重试
                            </Button>
                          }
                        />
                      ) : (
                        <Alert
                          type="info"
                          showIcon
                          title={
                            item.sourceTable
                              ? '正在读取该表的字段结构…'
                              : '选择源表后会读取它的字段结构；新表在任务启动时按源表结构自动创建。'
                          }
                        />
                      )}
                    </>
                  )
                };
              })}
            />
          </Space>
        );
      }}
    </Form.List>
  );

  const renderSummary = (values: SyncTaskGroupForm) => {
    const databaseScope = values.syncScope === 'DATABASE';
    const items = values.items || [];
    const source = sourceOf(values.sourceId);
    const target = sourceOf(values.targetId);
    const summaryItems = [
      {
        key: 'source',
        label: '源端',
        children: source
          ? `${source.sourceName} / ${(databaseScope && values.sourceDatabase) || probe.sourceDatabase || source.databaseName || '-'}`
          : '-'
      },
      { key: 'target', label: '目标端', children: target ? `${target.sourceName}（${target.sourceType}）` : '-' },
      {
        key: 'scope',
        label: '同步粒度',
        children: databaseScope
          ? `整库 · 自动发现新表${values.autoDiscover === '1' ? '开启' : '关闭'}`
          : `多表 · ${items.length} 张`
      },
      { key: 'mode', label: '同步模式', children: SYNC_MODE_LABELS[values.syncMode || ''] || values.syncMode || '-' },
      ...(target?.sourceType === 'KAFKA'
        ? [{ key: 'format', label: '输出格式', children: kafkaOutputFormatLabel(values.kafkaOutputFormat) }]
        : []),
      { key: 'ddl', label: '结构变更', children: '源表结构变化时暂停该表，修复后逐表恢复' },
      ...(!databaseScope && items.length
        ? [
            {
              key: 'items',
              label: '表项',
              span: 2,
              children: (
                <Space wrap size={[4, 4]}>
                  {items.map(item => (
                    <Tag key={item.sourceTable}>
                      {item.sourceTable} →{' '}
                      {target?.sourceType === 'POSTGRESQL' ? `${item.targetSchema || 'public'}.` : ''}
                      {item.targetTable}
                    </Tag>
                  ))}
                </Space>
              )
            }
          ]
        : [])
    ];
    // Whole-database tables are discovered on the server, so only listed tables can be checked here.
    const timeColumns = databaseScope
      ? []
      : items.flatMap(item => {
          const metadata = item.sourceTable ? tableMetadata[metadataKey(values.sourceId, item.sourceTable)] : undefined;
          return fractionalTimeColumns(metadata, columnList(item.selectedColumns)).map(
            column => `${item.sourceTable}.${column}`
          );
        });
    const timeWarning = timePrecisionWarning(timeColumns, values.syncMode, target?.sourceType);
    return (
      <>
        <Descriptions size="small" column={2} bordered items={summaryItems} />
        {timeWarning && <Alert type="warning" showIcon title={timeWarning} style={{ marginTop: 12 }} />}
      </>
    );
  };

  return (
    <ModalForm<SyncTaskGroupForm>
      title={groupId ? '修改多表同步任务' : '新增多表同步任务'}
      open={open}
      form={form}
      // Prefixes the field ids: the list's search form has its own groupName input, and a shared
      // id sent the labels (and scrollToField) to that one instead.
      name="syncGroupWizard"
      preserve
      layout="vertical"
      width={880}
      modalProps={modalProps}
      onValuesChange={() => setDirty(true)}
      onFinish={submitForm}
      onFinishFailed={() => message.error('请先完善当前步骤的必填项')}
      submitter={{
        render: () => (
          <WizardFooter
            step={wizardStep}
            lastStep={LAST_STEP}
            busy={busy}
            submitting={submitLoading}
            submitText={groupId ? '确认保存' : '确认创建'}
            onCancel={confirmClose}
            onPrev={() => setWizardStep(current => current - 1)}
            onNext={() => void nextWizardStep()}
            onSubmit={() => form.submit()}
          />
        )
      }}
    >
      <ProFormText name="groupId" hidden />
      <WizardSteps steps={WIZARD_STEPS} current={wizardStep} probePhase={busyPhase} />
      {wizardStep === 0 && (
        <>
          <Alert
            type="info"
            showIcon
            title="选择数据源后会重新连接并探查元数据，不直接信任历史连接状态。"
            style={{ marginBottom: 16 }}
          />
          <ProFormSelect
            name="sourceId"
            label="源数据源（MySQL）"
            options={sourceOptions}
            rules={[{ required: true, message: '请选择源数据源' }]}
            fieldProps={{
              loading: dataSources.length === 0,
              onChange: value => changeSource(value as string | number)
            }}
          />
          <SourceProbeResult
            probe={probe}
            databaseNote="多表同步从数据源配置的默认数据库选表，整库同步可在下一步改选。"
          />
          <ProFormSelect
            name="targetId"
            label="目标数据源（MySQL / PostgreSQL / Kafka）"
            options={targetOptions}
            rules={[{ required: true, message: '请选择目标数据源' }]}
            fieldProps={{ onChange: value => changeTarget(value as string | number) }}
          />
          {kafkaTarget && (
            <Form.Item
              name="kafkaOutputFormat"
              label="Kafka 输出格式（组内全部表项统一）"
              rules={[{ required: true, message: '请选择输出格式' }]}
              extra="组级配置：整库同步自动发现的新表同样继承该格式。消息 Key 始终为同步键 JSON，分区与顺序不变。"
            >
              <Select options={kafkaOutputFormatOptions} placeholder="默认JSON" />
            </Form.Item>
          )}
        </>
      )}
      {wizardStep === 1 && (
        <>
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
          {syncScope === 'DATABASE' ? (
            <>
              <Form.Item name="sourceDatabase" label="源数据库" extra="留空时使用数据源配置的默认数据库。">
                <Select
                  allowClear
                  showSearch
                  options={probe.sourceDatabases.map(database => ({ label: database, value: database }))}
                  placeholder={`默认：${probe.sourceDatabase || '数据源数据库'}`}
                />
              </Form.Item>
              <Form.Item
                name="autoDiscover"
                label="自动发现新表"
                // Stored as '1' / '0' like the API, so the value survives this step unmounting.
                getValueProps={value => ({ checked: value === '1' })}
                normalize={checked => (checked ? '1' : '0')}
                extra="开启后，运行中的任务组会定期扫描源库，新建的表自动加入并开始同步。"
              >
                <Switch checkedChildren="开启" unCheckedChildren="关闭" />
              </Form.Item>
              {kafkaTarget && (
                <Alert
                  type="info"
                  showIcon
                  title="整库同步到 Kafka：平台按源表名自动创建 topic（1 分区），发现的每张新表同样自动建 topic。"
                />
              )}
            </>
          ) : (
            renderItems()
          )}
        </>
      )}
      {wizardStep === 2 && (
        <>
          <ProFormText name="groupName" label="任务组名称" rules={[{ required: true, message: '请输入任务组名称' }]} />
          <Form.Item name="syncMode" label="同步模式" rules={[{ required: true, message: '请选择同步模式' }]}>
            <Radio.Group aria-label="同步模式">
              <Radio value="FULL">全量同步</Radio>
              <Radio value="INCREMENTAL">纯增量</Radio>
              <Radio value="FULL_CDC">全量 + CDC</Radio>
            </Radio.Group>
          </Form.Item>
          <Form.Item noStyle shouldUpdate={(prev, current) => prev.syncMode !== current.syncMode}>
            {({ getFieldValue }) =>
              getFieldValue('syncMode') === 'INCREMENTAL' ? (
                <>
                  <Alert
                    type="warning"
                    showIcon
                    title="纯增量不会补齐任务组创建前的历史数据，目标端必须已有可信基线。"
                    style={{ marginBottom: 12 }}
                  />
                  {probe.cdcPrecheck && !probe.cdcPrecheck.passed && (
                    <Alert
                      type="error"
                      showIcon
                      title="CDC 前置检查未通过，不能创建纯增量任务组。"
                      description={probe.cdcPrecheck.message}
                      style={{ marginBottom: 12 }}
                    />
                  )}
                </>
              ) : null
            }
          </Form.Item>
          <Divider titlePlacement="left" plain>
            源库保护（每个表项）
          </Divider>
          <Space wrap align="start">
            <ProFormDigit
              name="readLimitRowsPerSecond"
              label="最大行数/秒"
              min={1}
              max={100000}
              rules={[{ required: true }]}
              fieldProps={{ style: { width: 170 } }}
            />
            <ProFormDigit
              name="readLimitBytesPerSecond"
              label="最大字节/秒"
              min={1}
              max={1073741824}
              rules={[{ required: true }]}
              fieldProps={{ style: { width: 190 } }}
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
              fieldProps={{ style: { width: 170 } }}
            />
          </Space>
          <Alert type="info" showIcon title="任务组按表提交独立作业，以上限速与连接数分别作用于每个表项。" />
          <Divider titlePlacement="left" plain>
            提交确认
          </Divider>
          <Form.Item noStyle shouldUpdate>
            {({ getFieldsValue }) => renderSummary(getFieldsValue(true) as SyncTaskGroupForm)}
          </Form.Item>
        </>
      )}
    </ModalForm>
  );
}
