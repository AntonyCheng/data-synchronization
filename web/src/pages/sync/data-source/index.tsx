import { DeleteOutlined, EditOutlined, KeyOutlined, PlusOutlined, SafetyCertificateOutlined } from '@ant-design/icons';
import {
  ModalForm,
  PageContainer,
  ProFormDigit,
  ProFormDependency,
  ProFormSelect,
  ProFormText,
  ProFormTextArea,
  ProTable,
  type ActionType,
  type ProColumns
} from '@ant-design/pro-components';
import { useBoolean } from 'ahooks';
import { Alert, AutoComplete, Button, Form, message, Modal, Popconfirm, Tag } from 'antd';
import { useRef, useState } from 'react';
import type {
  ConnectionTestResult,
  DataSourceForm,
  DataSourceQuery,
  DataSourceVO,
  KafkaSecurityProtocol
} from '@/api/sync/data-source/types';
import {
  addDataSource,
  deleteDataSource,
  getDataSource,
  listDataSources,
  migrateDataSourceCredentials,
  testDataSource,
  testUnsavedDataSource,
  updateDataSource
} from '@/api/sync/data-source';
import RowActions from '@/components/common/RowActions';
import { useLoading } from '@/hooks/useLoading';
import { useTableScroll } from '@/hooks/useTableScroll';
import { useUserStore } from '@/stores/userStore';
import { hasPermi } from '@/utils/permission';
import { toPageQuery, toTableData } from '@/utils/ruoyi';

const defaultForm: DataSourceForm = {
  sourceType: 'MYSQL',
  port: 3306,
  sslEnabled: '0',
  status: '0',
  kafkaSecurityProtocol: 'PLAINTEXT',
  kafkaSslVerifyHostname: '1'
};

const DEFAULT_PORTS = { MYSQL: 3306, POSTGRESQL: 5432, KAFKA: 9092 };

const KAFKA_PROTOCOLS: { label: string; value: KafkaSecurityProtocol }[] = [
  { label: '无认证（PLAINTEXT）', value: 'PLAINTEXT' },
  { label: 'SASL（SASL_PLAINTEXT）', value: 'SASL_PLAINTEXT' },
  { label: 'SASL + SSL（SASL_SSL）', value: 'SASL_SSL' },
  { label: 'SSL（SSL，可双向认证）', value: 'SSL' }
];
const usesSasl = (protocol?: string) => protocol === 'SASL_PLAINTEXT' || protocol === 'SASL_SSL';
const usesSsl = (protocol?: string) => protocol === 'SSL' || protocol === 'SASL_SSL';
const PEM_TEXTAREA = { autoSize: { minRows: 3, maxRows: 8 }, style: { fontFamily: 'monospace', fontSize: 12 } };

/** Blank server time zone: what the engine is told for a MySQL source (the pre-configurable value). */
const COMPATIBLE_TIME_ZONE = 'Asia/Shanghai';

/** Offered by the time-zone field; any other IANA id can be typed in. */
const COMMON_TIME_ZONES = [
  { value: 'Asia/Shanghai', label: 'Asia/Shanghai（中国，UTC+8）' },
  { value: 'UTC', label: 'UTC（UTC+0）' },
  { value: 'Asia/Hong_Kong', label: 'Asia/Hong_Kong（UTC+8）' },
  { value: 'Asia/Taipei', label: 'Asia/Taipei（UTC+8）' },
  { value: 'Asia/Singapore', label: 'Asia/Singapore（UTC+8）' },
  { value: 'Asia/Tokyo', label: 'Asia/Tokyo（UTC+9）' },
  { value: 'Asia/Seoul', label: 'Asia/Seoul（UTC+9）' },
  { value: 'Asia/Kolkata', label: 'Asia/Kolkata（印度，UTC+5:30）' },
  { value: 'Asia/Dubai', label: 'Asia/Dubai（UTC+4）' },
  { value: 'Europe/Moscow', label: 'Europe/Moscow（UTC+3）' },
  { value: 'Europe/Berlin', label: 'Europe/Berlin（UTC+1，夏令时 +2）' },
  { value: 'Europe/London', label: 'Europe/London（UTC+0，夏令时 +1）' },
  { value: 'America/New_York', label: 'America/New_York（UTC-5，夏令时 -4）' },
  { value: 'America/Chicago', label: 'America/Chicago（UTC-6，夏令时 -5）' },
  { value: 'America/Los_Angeles', label: 'America/Los_Angeles（UTC-8，夏令时 -7）' },
  { value: 'Australia/Sydney', label: 'Australia/Sydney（UTC+10，夏令时 +11）' },
  { value: 'Etc/GMT-8', label: 'Etc/GMT-8（固定 UTC+8，无夏令时）' }
];

/**
 * A quick hint for abbreviations, which name several zones (CST: China / US Central / Cuba). The
 * backend is authoritative: it accepts IANA ids only and rejects every abbreviation Java knows.
 */
const validateTimeZone = async (_: unknown, value?: string) => {
  const zone = value?.trim();
  if (!zone || ['UTC', 'GMT'].includes(zone)) return;
  if (/^[A-Z]{2,5}$/.test(zone)) {
    throw new Error('时区缩写含义不唯一（例如 CST 可以是中国、美国中部或古巴），请填写 IANA 时区 ID，如 Asia/Shanghai');
  }
  if (!/^[A-Za-z][A-Za-z0-9_+\-/]*$/.test(zone)) {
    throw new Error('请填写 IANA 时区 ID，如 UTC、Asia/Shanghai；固定偏移用 Etc/GMT-8 表示 UTC+8');
  }
};

const timeZoneHelp =
  '源库渲染 TIMESTAMP 所用的时区，告诉引擎如何解释增量（binlog）阶段的 TIMESTAMP。留空为兼容模式，按 Asia/Shanghai 解释；' +
  '与源库实际时区不一致时，增量阶段的 TIMESTAMP 会整体偏移（DATETIME、DATE、TIME 不受影响）。' +
  '修改后，读取此数据源的 CDC 任务需要重新初始化才能继续（原位点按旧时区记录）；运行中的任务需先暂停或停止。';

export default function SyncDataSourcePage() {
  const actionRef = useRef<ActionType | undefined>(undefined);
  const { tableScroll } = useTableScroll(680);
  const [form] = Form.useForm<DataSourceForm>();
  const userInfo = useUserStore(state => state.userInfo);
  const [modalOpen, { setTrue: openModal, setFalse: closeModal }] = useBoolean(false);
  const [modalTitle, setModalTitle] = useState('');
  const [formTest, setFormTest] = useState<ConnectionTestResult>();
  const { loading: formTesting, withLoading: withFormTesting } = useLoading();

  const canAdd = hasPermi(userInfo, ['sync:data-source:add']);
  const canEdit = hasPermi(userInfo, ['sync:data-source:edit']);
  const canRemove = hasPermi(userInfo, ['sync:data-source:remove']);
  const canTest = hasPermi(userInfo, ['sync:data-source:test']);
  const canMigrateCredentials = hasPermi(userInfo, ['sync:data-source:credential-migrate']);

  const openAdd = () => {
    form.resetFields();
    form.setFieldsValue(defaultForm);
    setFormTest(undefined);
    setModalTitle('新增数据源');
    openModal();
  };

  const openEdit = async (row: DataSourceVO) => {
    const res = await getDataSource(row.sourceId);
    form.resetFields();
    form.setFieldsValue({
      ...res.data,
      password: undefined,
      kafkaSslClientKey: undefined,
      kafkaSecurityProtocol: res.data.kafkaSecurityProtocol || 'PLAINTEXT',
      kafkaSslVerifyHostname: res.data.kafkaSslVerifyHostname || '1'
    });
    setFormTest(undefined);
    setModalTitle('修改数据源');
    openModal();
  };

  /** Only MySQL sources carry a zone; an empty string (not an omitted field) clears it back to compatibility mode. */
  const toPayload = (values: DataSourceForm): DataSourceForm => ({
    ...values,
    serverTimeZone: values.sourceType === 'MYSQL' ? values.serverTimeZone?.trim() || '' : ''
  });

  const submitForm = async (values: DataSourceForm) => {
    const payload = toPayload(values);
    if (payload.sourceId) await updateDataSource(payload);
    else await addDataSource(payload);
    message.success('保存成功');
    closeModal();
    actionRef.current?.reload();
    return true;
  };

  /** Tests what is in the form (a blank password on edit keeps the stored one) and reads the source's time zone. */
  const testForm = () =>
    withFormTesting(async () => {
      await form.validateFields([
        'sourceType',
        'host',
        'port',
        'databaseName',
        'username',
        'serverTimeZone',
        'kafkaSaslMechanism'
      ]);
      const values = toPayload(form.getFieldsValue(true) as DataSourceForm);
      const result = values.sourceId
        ? await testDataSource(values.sourceId, values)
        : await testUnsavedDataSource(values);
      setFormTest(result.data);
    });

  const remove = async (row: DataSourceVO) => {
    await deleteDataSource(row.sourceId);
    message.success('删除成功');
    actionRef.current?.reloadAndRest?.();
  };

  const test = async (row: DataSourceVO) => {
    const result = await testDataSource(row.sourceId);
    const { success, message: detail, latencyMs, timeZone } = result.data;
    const name = row.sourceName || `数据源 ${row.sourceId}`;
    const summary = success ? `数据源“${name}”连接测试成功` : `数据源“${name}”连接测试失败`;
    message[success ? 'success' : 'error'](`${summary}${success ? `（耗时 ${latencyMs}ms）` : ''}：${detail}`);
    if (success && timeZone && timeZone.matched !== true) {
      message.warning(`数据源“${name}”作为 CDC 源端时：${timeZone.message}。请在“修改”中设置服务器时区。`, 8);
    }
  };

  const migrateCredentials = () => {
    Modal.confirm({
      title: '迁移数据源凭证',
      content: '将现有数据源密码按当前加密密钥重新写入。请确认已完成元数据库备份。',
      okText: '开始迁移',
      cancelText: '取消',
      onOk: async () => {
        const result = await migrateDataSourceCredentials();
        message.success(result.data.message);
      }
    });
  };

  const columns: ProColumns<DataSourceVO>[] = [
    { title: '名称', dataIndex: 'sourceName', width: 180 },
    {
      title: '类型',
      dataIndex: 'sourceType',
      width: 120,
      valueEnum: { MYSQL: 'MySQL', POSTGRESQL: 'PostgreSQL', KAFKA: 'Kafka' }
    },
    { title: '地址', dataIndex: 'host' },
    { title: '端口', dataIndex: 'port', width: 90, search: false },
    { title: '数据库', dataIndex: 'databaseName' },
    { title: '用户名', dataIndex: 'username', search: false },
    {
      title: '认证',
      dataIndex: 'kafkaSecurityProtocol',
      width: 170,
      search: false,
      render: (_, row) => {
        if (row.sourceType !== 'KAFKA') return '-';
        const protocol = row.kafkaSecurityProtocol || 'PLAINTEXT';
        return usesSasl(protocol) ? `${protocol} / ${row.kafkaSaslMechanism ?? ''}` : protocol;
      }
    },
    {
      title: '服务器时区',
      dataIndex: 'serverTimeZone',
      width: 150,
      search: false,
      render: (_, row) =>
        row.sourceType !== 'MYSQL' ? '-' : row.serverTimeZone || <Tag>兼容：{COMPATIBLE_TIME_ZONE}</Tag>
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 90,
      valueEnum: { '0': '正常', '1': '停用' },
      render: (_, row) => (
        <Tag color={row.status === '0' ? 'success' : 'default'}>{row.status === '0' ? '正常' : '停用'}</Tag>
      )
    },
    {
      title: '操作',
      valueType: 'option',
      width: 150,
      fixed: 'right',
      render: (_, row) => (
        <RowActions
          actions={[
            canTest && {
              key: 'test',
              label: '测试连接',
              icon: <SafetyCertificateOutlined />,
              onClick: () => test(row)
            },
            canEdit && { key: 'edit', label: '修改', icon: <EditOutlined />, onClick: () => openEdit(row) },
            canRemove && {
              key: 'delete',
              label: '删除',
              icon: <DeleteOutlined />,
              danger: true,
              confirm: `是否确认删除数据源“${row.sourceName}”？`,
              onClick: () => remove(row)
            }
          ]}
        />
      )
    }
  ];

  return (
    <PageContainer title="数据源管理">
      <ProTable<DataSourceVO, DataSourceQuery>
        actionRef={actionRef}
        rowKey="sourceId"
        columns={columns}
        scroll={tableScroll}
        search={{ labelWidth: 80 }}
        pagination={{ defaultPageSize: 10, showSizeChanger: true }}
        request={async params => {
          const res = await listDataSources(toPageQuery(params));
          return toTableData(res);
        }}
        toolbar={{ title: '数据源列表' }}
        toolBarRender={() => [
          canAdd && (
            <Button key="add" type="primary" icon={<PlusOutlined />} onClick={openAdd}>
              新增数据源
            </Button>
          ),
          canMigrateCredentials && (
            <Button key="migrate-credentials" icon={<KeyOutlined />} onClick={migrateCredentials}>
              迁移凭证
            </Button>
          )
        ]}
      />
      <ModalForm<DataSourceForm>
        title={modalTitle}
        open={modalOpen}
        form={form}
        layout="vertical"
        width={640}
        modalProps={{ destroyOnHidden: true, onCancel: closeModal }}
        onOpenChange={open => !open && closeModal()}
        onValuesChange={changed => {
          // A port still at some type's default follows the type (Kafka 9092, not MySQL's 3306).
          if (changed.sourceType && Object.values(DEFAULT_PORTS).includes(form.getFieldValue('port'))) {
            form.setFieldValue('port', DEFAULT_PORTS[changed.sourceType as keyof typeof DEFAULT_PORTS]);
          }
          // A test result describes the endpoint it ran against; drop it once that changes.
          const endpointFields = [
            'sourceType',
            'host',
            'port',
            'databaseName',
            'username',
            'password',
            'sslEnabled',
            'kafkaSecurityProtocol',
            'kafkaSaslMechanism',
            'kafkaSslCaCert',
            'kafkaSslClientCert',
            'kafkaSslClientKey',
            'kafkaSslVerifyHostname'
          ];
          if (Object.keys(changed).some(key => endpointFields.includes(key))) setFormTest(undefined);
        }}
        onFinish={submitForm}
      >
        <ProFormText name="sourceId" hidden />
        <ProFormText name="sourceName" label="名称" rules={[{ required: true, message: '请输入数据源名称' }]} />
        <ProFormSelect
          name="sourceType"
          label="类型"
          options={[
            { label: 'MySQL', value: 'MYSQL' },
            { label: 'PostgreSQL', value: 'POSTGRESQL' },
            { label: 'Kafka', value: 'KAFKA' }
          ]}
          rules={[{ required: true, message: '请选择数据源类型' }]}
        />
        <ProFormDependency name={['sourceType']}>
          {({ sourceType }) =>
            sourceType === 'KAFKA' ? (
              <ProFormText
                name="host"
                label="Broker 地址"
                placeholder="kafka-1.example.com, kafka-2.example.com:9093"
                extra="多个 broker 用逗号分隔，可写成 主机:端口；没写端口的使用下面的端口"
                rules={[{ required: true, message: '请输入 broker 地址' }]}
              />
            ) : (
              <ProFormText name="host" label="主机地址" rules={[{ required: true, message: '请输入主机地址' }]} />
            )
          }
        </ProFormDependency>
        <ProFormDigit
          name="port"
          label="端口"
          min={1}
          max={65535}
          rules={[{ required: true, message: '请输入端口' }]}
        />
        <ProFormDependency name={['sourceType', 'kafkaSecurityProtocol', 'sourceId']}>
          {({ sourceType, kafkaSecurityProtocol, sourceId }) => {
            if (sourceType !== 'KAFKA') {
              return (
                <>
                  <ProFormText
                    name="databaseName"
                    label="数据库名称"
                    rules={[{ required: true, message: '请输入数据库名称' }]}
                  />
                  <ProFormText name="schemaName" label="Schema" />
                  <ProFormText name="username" label="用户名" rules={[{ required: true, message: '请输入用户名' }]} />
                  <ProFormText.Password
                    name="password"
                    label="密码"
                    placeholder={sourceId ? '留空表示保持原密码' : '请输入密码'}
                  />
                  <ProFormSelect
                    name="sslEnabled"
                    label="SSL"
                    options={[
                      { label: '关闭', value: '0' },
                      { label: '开启', value: '1' }
                    ]}
                  />
                </>
              );
            }
            const sasl = usesSasl(kafkaSecurityProtocol);
            const ssl = usesSsl(kafkaSecurityProtocol);
            return (
              <>
                <ProFormSelect
                  name="kafkaSecurityProtocol"
                  label="安全协议"
                  options={KAFKA_PROTOCOLS}
                  allowClear={false}
                  extra="与 Kafka 管理员确认：broker 监听器要求的认证方式"
                />
                {sasl && (
                  <>
                    <ProFormSelect
                      name="kafkaSaslMechanism"
                      label="SASL 机制"
                      options={['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'].map(value => ({ label: value, value }))}
                      rules={[{ required: true, message: '请选择 SASL 机制' }]}
                    />
                    <ProFormText
                      name="username"
                      label="用户名"
                      rules={[{ required: true, message: '请输入 SASL 用户名' }]}
                    />
                    <ProFormText.Password
                      name="password"
                      label="密码"
                      placeholder={sourceId ? '留空表示保持原密码' : '请输入 SASL 密码'}
                      rules={sourceId ? [] : [{ required: true, message: '请输入 SASL 密码' }]}
                    />
                  </>
                )}
                {ssl && (
                  <>
                    <ProFormTextArea
                      name="kafkaSslCaCert"
                      label="CA 证书（PEM）"
                      placeholder="-----BEGIN CERTIFICATE-----"
                      extra="签发 broker 证书的 CA；留空则只信任 JVM 内置的公共 CA"
                      fieldProps={PEM_TEXTAREA}
                    />
                    <ProFormTextArea
                      name="kafkaSslClientCert"
                      label="客户端证书（PEM，broker 要求双向认证时填写）"
                      placeholder="-----BEGIN CERTIFICATE-----"
                      fieldProps={PEM_TEXTAREA}
                    />
                    <ProFormTextArea
                      name="kafkaSslClientKey"
                      label="客户端私钥（未加密的 PKCS#8 PEM，与客户端证书一起填写）"
                      placeholder={sourceId ? '留空表示保持原私钥' : '-----BEGIN PRIVATE KEY-----'}
                      extra="加密保存，不会再显示；RSA/EC 格式可用 openssl pkcs8 -topk8 -nocrypt 转换"
                      fieldProps={PEM_TEXTAREA}
                    />
                    <ProFormSelect
                      name="kafkaSslVerifyHostname"
                      label="校验 broker 主机名"
                      allowClear={false}
                      options={[
                        { label: '校验（推荐）', value: '1' },
                        { label: '不校验（证书与访问地址不一致时）', value: '0' }
                      ]}
                    />
                  </>
                )}
                {canTest && (
                  <Form.Item>
                    <Button
                      icon={<SafetyCertificateOutlined />}
                      loading={formTesting}
                      onClick={() => void testForm().catch(() => undefined)}
                    >
                      测试连接
                    </Button>
                  </Form.Item>
                )}
                {formTest && (
                  <Alert
                    type={formTest.success ? 'success' : 'error'}
                    showIcon
                    style={{ marginBottom: 16 }}
                    title={formTest.success ? `连接成功（${formTest.latencyMs} ms）` : '连接失败'}
                    description={formTest.success ? undefined : formTest.message}
                  />
                )}
              </>
            );
          }}
        </ProFormDependency>
        <ProFormDependency name={['sourceType', 'serverTimeZone']}>
          {({ sourceType, serverTimeZone }) => {
            if (sourceType !== 'MYSQL') return null;
            const zone = formTest?.timeZone;
            const suggestion = zone?.suggestedTimeZone;
            const current = (serverTimeZone as string | undefined)?.trim() || '';
            return (
              <>
                <Form.Item
                  name="serverTimeZone"
                  label="服务器时区（仅作为同步源端时生效）"
                  rules={[{ validator: validateTimeZone }]}
                  extra={timeZoneHelp}
                >
                  <AutoComplete
                    options={COMMON_TIME_ZONES}
                    allowClear
                    placeholder={`留空 = 兼容模式：按 ${COMPATIBLE_TIME_ZONE}`}
                    filterOption={(input, option) =>
                      `${option?.value ?? ''} ${option?.label ?? ''}`.toLowerCase().includes(input.trim().toLowerCase())
                    }
                  />
                </Form.Item>
                {canTest && (
                  <Form.Item>
                    <Button
                      icon={<SafetyCertificateOutlined />}
                      loading={formTesting}
                      onClick={() => void testForm().catch(() => undefined)}
                    >
                      测试连接并检测源库时区
                    </Button>
                  </Form.Item>
                )}
                {formTest && (
                  <Alert
                    type={!formTest.success ? 'error' : zone && zone.matched !== true ? 'warning' : 'success'}
                    showIcon
                    style={{ marginBottom: 16 }}
                    title={formTest.success ? `连接成功（${formTest.latencyMs} ms）` : '连接失败'}
                    description={
                      !formTest.success
                        ? formTest.message
                        : zone && (
                            <>
                              <div>
                                源库时区：{zone.serverTimeZone || '未知'}
                                {zone.serverUtcOffset ? `，当前 ${zone.serverUtcOffset}` : ''}
                                {suggestion ? `；建议填写 ${suggestion}` : ''}
                              </div>
                              <div>{zone.message}</div>
                              {current.toLowerCase() !== (zone.configuredTimeZone || '').toLowerCase() && (
                                <div>
                                  表单中的时区已改动，以上结论按测试时的 {zone.effectiveTimeZone}
                                  得出；保存前可再次测试确认。
                                </div>
                              )}
                            </>
                          )
                    }
                    action={
                      formTest.success && suggestion && suggestion.toLowerCase() !== current.toLowerCase() ? (
                        <Button size="small" onClick={() => form.setFieldValue('serverTimeZone', suggestion)}>
                          填入 {suggestion}
                        </Button>
                      ) : undefined
                    }
                  />
                )}
              </>
            );
          }}
        </ProFormDependency>
        <ProFormSelect
          name="status"
          label="状态"
          options={[
            { label: '正常', value: '0' },
            { label: '停用', value: '1' }
          ]}
        />
        <ProFormText name="remark" label="备注" />
      </ModalForm>
    </PageContainer>
  );
}
