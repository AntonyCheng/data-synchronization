import {
  ModalForm,
  ProFormDependency,
  ProFormDigit,
  ProFormSelect,
  ProFormSwitch,
  ProFormText
} from '@ant-design/pro-components';
import { Alert, AutoComplete, Button, Checkbox, Divider, Form, Input, message, Radio, Select, Space } from 'antd';
import { useEffect, useState } from 'react';
import type { DataSourceMetadataVO, DataSourceVO } from '@/api/sync/data-source/types';
import type { SyncTaskGroupForm, SyncTaskGroupVO } from '@/api/sync/group/types';
import { createKafkaTopic, getDataSourceMetadata, listDataSourceTables, listKafkaTopics } from '@/api/sync/data-source';
import { addSyncTaskGroup, getSyncTaskGroup, updateSyncTaskGroup } from '@/api/sync/group';
import { emptyForm, kafkaOutputFormatOptions, keyOptions, sourceTypeOf } from '@/pages/sync/group/shared';

export interface GroupFormModalProps {
  open: boolean;
  /** Set to edit that group, left out to create a new one. */
  group?: SyncTaskGroupVO;
  dataSources: DataSourceVO[];
  onClose: () => void;
  onSaved: () => void;
}

/**
 * Create / edit form for a task group: scope (multi-table vs whole-database), source and
 * target, per-item source protection, and — for multi-table — the table list with its
 * column selection. It owns the form and the metadata it introspects per item.
 */
export default function GroupFormModal({ open, group, dataSources, onClose, onSaved }: GroupFormModalProps) {
  const [form] = Form.useForm<SyncTaskGroupForm>();
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [targetTables, setTargetTables] = useState<string[]>([]);
  const [targetTablesLoading, setTargetTablesLoading] = useState(false);
  const [topicCreateLoading, setTopicCreateLoading] = useState<number>();
  const [itemMetadata, setItemMetadata] = useState<Record<string, DataSourceMetadataVO>>({});

  const syncScope = Form.useWatch('syncScope', form) || 'MULTI_TABLE';
  const targetId = Form.useWatch('targetId', form);
  const targetType = sourceTypeOf(dataSources, targetId);
  const kafkaTarget = targetType === 'KAFKA';
  const postgresTarget = targetType === 'POSTGRESQL';
  const groupId = group?.groupId;

  useEffect(() => {
    if (!open) return;
    form.resetFields();
    setItemMetadata({});
    setTargetTables([]);
    if (!groupId) {
      form.setFieldsValue(emptyForm);
      return;
    }
    void getSyncTaskGroup(groupId).then(async result => {
      form.setFieldsValue(result.data);
      await loadTables(result.data.sourceId);
      await loadTargetTables(result.data.targetId);
      await Promise.all(
        (result.data.items || []).map((item, index) => loadItemMetadata(index, item.sourceTable, result.data.sourceId))
      );
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, groupId]);

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
    const source = dataSources.find(
      item => String(item.sourceId) === String(sourceId ?? form.getFieldValue('sourceId'))
    );
    if (!source || !tableName) return;
    const result = await getDataSourceMetadata(source.sourceId, source.databaseName || '', tableName);
    setItemMetadata(current => ({ ...current, [String(itemIndex)]: result.data }));
    const selectedColumns = form.getFieldValue(['items', itemIndex, 'selectedColumns']);
    if (typeof selectedColumns === 'string')
      form.setFieldValue(['items', itemIndex, 'selectedColumns'], selectedColumns.split(',').filter(Boolean));
    else if (!selectedColumns)
      form.setFieldValue(
        ['items', itemIndex, 'selectedColumns'],
        result.data.columns.map(column => column.name)
      );
    if (!form.getFieldValue(['items', itemIndex, 'syncKeyColumns']))
      form.setFieldValue(['items', itemIndex, 'syncKeyColumns'], keyOptions(result.data)[0]?.value);
  };

  const submit = async (values: SyncTaskGroupForm) => {
    const databaseScope = values.syncScope === 'DATABASE';
    const payload = {
      ...values,
      kafkaOutputFormat: sourceTypeOf(dataSources, values.targetId) === 'KAFKA' ? values.kafkaOutputFormat : undefined,
      // Form.List values remain mounted when users switch to whole-database mode.
      // Whole-database groups discover their items server-side and must not submit them.
      items: databaseScope
        ? []
        : values.items?.map(item => ({
            ...item,
            selectedColumns: Array.isArray(item.selectedColumns) ? item.selectedColumns.join(',') : item.selectedColumns
          }))
    };
    if (values.groupId) await updateSyncTaskGroup(payload);
    else await addSyncTaskGroup(payload);
    message.success('保存成功');
    onClose();
    onSaved();
    return true;
  };

  const sourceOptions = dataSources
    .filter(item => item.sourceType === 'MYSQL')
    .map(item => ({ label: `${item.sourceName} (${item.databaseName})`, value: item.sourceId }));
  const targetOptions = dataSources
    .filter(item => item.sourceType === 'POSTGRESQL' || item.sourceType === 'MYSQL' || item.sourceType === 'KAFKA')
    .map(item => ({ label: `${item.sourceName} (${item.databaseName})`, value: item.sourceId }));

  return (
    <ModalForm<SyncTaskGroupForm>
      title={groupId ? '修改多表同步任务' : '新增多表同步任务'}
      open={open}
      form={form}
      width={880}
      layout="vertical"
      modalProps={{ destroyOnHidden: true, onCancel: onClose }}
      onOpenChange={isOpen => {
        if (!isOpen) onClose();
      }}
      onFinish={submit}
    >
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
      <Space style={{ width: '100%' }} align="start">
        <ProFormSelect
          name="sourceId"
          label="源数据源（MySQL）"
          options={sourceOptions}
          rules={[{ required: true }]}
          fieldProps={{ style: { width: 300 }, onChange: value => void loadTables(value as string | number) }}
        />
        <ProFormDependency name={['sourceId']}>
          {({ sourceId }) => (
            <ProFormSelect
              name="targetId"
              label="目标数据源（MySQL / PostgreSQL / Kafka）"
              options={targetOptions.filter(option => String(option.value) !== String(sourceId))}
              rules={[{ required: true }]}
              fieldProps={{
                style: { width: 300 },
                onChange: value => {
                  const type = sourceTypeOf(dataSources, value as string | number);
                  const items = form.getFieldValue('items') || [];
                  form.setFieldsValue({
                    items: items.map((item: NonNullable<SyncTaskGroupForm['items']>[number]) => ({
                      ...item,
                      targetSchema: type === 'POSTGRESQL' ? item.targetSchema || 'public' : undefined
                    }))
                  });
                  setTargetTables([]);
                  void loadTargetTables(value as string | number);
                }
              }}
            />
          )}
        </ProFormDependency>
      </Space>
      {kafkaTarget && (
        <Form.Item
          name="kafkaOutputFormat"
          label="Kafka 输出格式（组内全部表项统一）"
          rules={[{ required: true, message: '请选择输出格式' }]}
          extra="组级配置：整库同步自动发现的新表同样继承该格式。消息 Key 始终为同步键 JSON，分区与顺序不变。"
        >
          <Select options={kafkaOutputFormatOptions} style={{ width: 300 }} placeholder="默认JSON" />
        </Form.Item>
      )}
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
      <Alert type="info" showIcon message="任务组按表提交独立作业，以上限速与连接数分别作用于每个表项。" />
      {syncScope === 'DATABASE' ? (
        <Space direction="vertical" style={{ width: '100%' }} size={8}>
          <Space style={{ width: '100%' }} align="start">
            <ProFormText
              name="sourceDatabase"
              label="源数据库"
              placeholder="默认使用数据源数据库"
              fieldProps={{ style: { width: 300 } }}
            />
            <ProFormSwitch
              name="autoDiscover"
              label="自动发现新表"
              fieldProps={{ checkedChildren: '开启', unCheckedChildren: '关闭' }}
              convertValue={value => value === '1'}
              transform={value => ({ autoDiscover: value ? '1' : '0' })}
            />
          </Space>
          {kafkaTarget && (
            <Alert
              type="info"
              showIcon
              message="整库同步到 Kafka：平台按源表名自动创建 topic（1 分区），发现的每张新表同样自动建 topic。"
            />
          )}
        </Space>
      ) : (
        <Form.List name="items">
          {(fields, { add, remove }) => (
            <Space direction="vertical" style={{ width: '100%' }} size={8}>
              {fields.map(field => {
                const metadata = itemMetadata[String(field.name)];
                return (
                  <div key={field.key} style={{ borderBottom: '1px solid #f0f0f0', paddingBottom: 12 }}>
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
                              const items: NonNullable<SyncTaskGroupForm['items']> = form.getFieldValue('items') || [];
                              const duplicate = items.some(
                                (item, index) => index !== field.name && item?.sourceTable === value
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
                          options={sourceTables.map(table => ({ label: table, value: table }))}
                          style={{ width: 220 }}
                          onChange={value => void loadItemMetadata(field.name, value)}
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
                      >
                        <AutoComplete
                          options={targetTables.map(table => ({ label: table, value: table }))}
                          allowClear
                          placeholder={
                            kafkaTarget
                              ? targetTablesLoading
                                ? '正在读取 topic 列表'
                                : '选择已有 topic 或输入新 topic'
                              : targetTablesLoading
                                ? '正在读取目标表，可直接输入新表名'
                                : '选择已有表或输入新表名'
                          }
                          style={{ width: 220 }}
                        />
                      </Form.Item>
                      {kafkaTarget && (
                        <Button
                          type="default"
                          loading={topicCreateLoading === field.name}
                          onClick={() => void createGroupTopic(field.name)}
                        >
                          创建 topic
                        </Button>
                      )}
                      <Button danger type="text" onClick={() => remove(field.name)}>
                        删除
                      </Button>
                    </Space>
                    <Alert
                      type="info"
                      showIcon
                      message={
                        kafkaTarget
                          ? '可选择已有 topic，也可输入名称后点击“创建 topic”；平台不会依赖 broker 自动建 topic。'
                          : '可选择目标库已有表，也可直接输入新表名；新表启动时按源表字段结构自动创建。'
                      }
                      style={{ marginTop: 8 }}
                    />
                    {metadata && (
                      <Space direction="vertical" size={4} style={{ width: '100%' }}>
                        <Form.Item
                          {...field}
                          name={[field.name, 'selectedColumns']}
                          label="同步字段"
                          rules={[{ required: true, message: '至少选择一个同步字段' }]}
                          extra="同步键字段不可排除。"
                        >
                          <Checkbox.Group
                            options={metadata.columns.map(column => ({
                              label: `${column.name} (${column.typeName || '-'})`,
                              value: column.name
                            }))}
                          />
                        </Form.Item>
                        <Form.Item
                          {...field}
                          name={[field.name, 'syncKeyColumns']}
                          label="同步键"
                          rules={[{ required: true, message: '请选择可靠同步键' }]}
                        >
                          <Select
                            options={keyOptions(metadata)}
                            disabled={keyOptions(metadata).length === 0}
                            placeholder="请选择主键或非空唯一键"
                          />
                        </Form.Item>
                        {keyOptions(metadata).length === 0 && (
                          <Alert type="warning" showIcon message="该表没有可靠同步键，无法加入 CDC 任务组。" />
                        )}
                      </Space>
                    )}
                  </div>
                );
              })}
              <Button type="dashed" onClick={() => add({ targetSchema: postgresTarget ? 'public' : undefined })}>
                添加表
              </Button>
            </Space>
          )}
        </Form.List>
      )}
    </ModalForm>
  );
}
