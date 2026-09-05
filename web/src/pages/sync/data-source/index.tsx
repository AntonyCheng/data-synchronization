import { DeleteOutlined, EditOutlined, KeyOutlined, PlusOutlined, SafetyCertificateOutlined } from '@ant-design/icons';
import {
  ModalForm,
  PageContainer,
  ProFormDigit,
  ProFormDependency,
  ProFormSelect,
  ProFormText,
  ProTable,
  type ActionType,
  type ProColumns
} from '@ant-design/pro-components';
import { useBoolean } from 'ahooks';
import { Button, Form, message, Modal, Popconfirm, Tag } from 'antd';
import { useRef, useState } from 'react';
import type { DataSourceForm, DataSourceQuery, DataSourceVO } from '@/api/sync/data-source/types';
import {
  addDataSource,
  deleteDataSource,
  getDataSource,
  listDataSources,
  migrateDataSourceCredentials,
  testDataSource,
  updateDataSource
} from '@/api/sync/data-source';
import RowActions from '@/components/common/RowActions';
import { useTableScroll } from '@/hooks/useTableScroll';
import { useUserStore } from '@/stores/userStore';
import { hasPermi } from '@/utils/permission';
import { toPageQuery, toTableData } from '@/utils/ruoyi';

const defaultForm: DataSourceForm = { sourceType: 'MYSQL', port: 3306, sslEnabled: '0', status: '0' };

export default function SyncDataSourcePage() {
  const actionRef = useRef<ActionType | undefined>(undefined);
  const { tableScroll } = useTableScroll(680);
  const [form] = Form.useForm<DataSourceForm>();
  const userInfo = useUserStore(state => state.userInfo);
  const [modalOpen, { setTrue: openModal, setFalse: closeModal }] = useBoolean(false);
  const [modalTitle, setModalTitle] = useState('');

  const canAdd = hasPermi(userInfo, ['sync:data-source:add']);
  const canEdit = hasPermi(userInfo, ['sync:data-source:edit']);
  const canRemove = hasPermi(userInfo, ['sync:data-source:remove']);
  const canTest = hasPermi(userInfo, ['sync:data-source:test']);
  const canMigrateCredentials = hasPermi(userInfo, ['sync:data-source:credential-migrate']);

  const openAdd = () => {
    form.resetFields();
    form.setFieldsValue(defaultForm);
    setModalTitle('新增数据源');
    openModal();
  };

  const openEdit = async (row: DataSourceVO) => {
    const res = await getDataSource(row.sourceId);
    form.resetFields();
    form.setFieldsValue({ ...res.data, password: undefined });
    setModalTitle('修改数据源');
    openModal();
  };

  const submitForm = async (values: DataSourceForm) => {
    if (values.sourceId) await updateDataSource(values);
    else await addDataSource(values);
    message.success('保存成功');
    closeModal();
    actionRef.current?.reload();
    return true;
  };

  const remove = async (row: DataSourceVO) => {
    await deleteDataSource(row.sourceId);
    message.success('删除成功');
    actionRef.current?.reloadAndRest?.();
  };

  const test = async (row: DataSourceVO) => {
    const result = await testDataSource(row.sourceId);
    const { success, message: detail, latencyMs } = result.data;
    const name = row.sourceName || `数据源 ${row.sourceId}`;
    const summary = success ? `数据源“${name}”连接测试成功` : `数据源“${name}”连接测试失败`;
    message[success ? 'success' : 'error'](
      `${summary}${success ? `（耗时 ${latencyMs}ms）` : ''}：${detail}`
    );
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
    { title: '类型', dataIndex: 'sourceType', width: 120, valueEnum: { MYSQL: 'MySQL', POSTGRESQL: 'PostgreSQL', KAFKA: 'Kafka' } },
    { title: '地址', dataIndex: 'host' },
    { title: '端口', dataIndex: 'port', width: 90, search: false },
    { title: '数据库', dataIndex: 'databaseName' },
    { title: '用户名', dataIndex: 'username', search: false },
    {
      title: '状态',
      dataIndex: 'status',
      width: 90,
      valueEnum: { '0': '正常', '1': '停用' },
      render: (_, row) => <Tag color={row.status === '0' ? 'success' : 'default'}>{row.status === '0' ? '正常' : '停用'}</Tag>
    },
    {
      title: '操作',
      valueType: 'option',
      width: 150,
      fixed: 'right',
      render: (_, row) => (
        <RowActions
          actions={[
            canTest && { key: 'test', label: '测试连接', icon: <SafetyCertificateOutlined />, onClick: () => test(row) },
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
          canMigrateCredentials && <Button key="migrate-credentials" icon={<KeyOutlined />} onClick={migrateCredentials}>迁移凭证</Button>
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
        onFinish={submitForm}
      >
        <ProFormText name="sourceId" hidden />
        <ProFormText name="sourceName" label="名称" rules={[{ required: true, message: '请输入数据源名称' }]} />
        <ProFormSelect
          name="sourceType"
          label="类型"
          options={[{ label: 'MySQL', value: 'MYSQL' }, { label: 'PostgreSQL', value: 'POSTGRESQL' }, { label: 'Kafka', value: 'KAFKA' }]}
          rules={[{ required: true, message: '请选择数据源类型' }]}
        />
        <ProFormText name="host" label="主机地址" rules={[{ required: true, message: '请输入主机地址' }]} />
        <ProFormDigit name="port" label="端口" min={1} max={65535} rules={[{ required: true, message: '请输入端口' }]} />
        <ProFormDependency name={['sourceType']}>
          {({ sourceType }) => (
            <ProFormText
              name="databaseName"
              label={sourceType === 'KAFKA' ? '数据库名称（Kafka 不需要）' : '数据库名称'}
              rules={sourceType === 'KAFKA' ? [] : [{ required: true, message: '请输入数据库名称' }]}
            />
          )}
        </ProFormDependency>
        <ProFormText name="schemaName" label="Schema" />
        <ProFormText name="username" label="用户名" rules={[{ required: true, message: '请输入用户名' }]} />
        <ProFormDependency name={['sourceId']}>
          {({ sourceId }) => (
            <ProFormText.Password
              name="password"
              label="密码"
              placeholder={sourceId ? '留空表示保持原密码' : '请输入密码'}
            />
          )}
        </ProFormDependency>
        <ProFormSelect
          name="sslEnabled"
          label="SSL"
          options={[{ label: '关闭', value: '0' }, { label: '开启', value: '1' }]}
        />
        <ProFormSelect
          name="status"
          label="状态"
          options={[{ label: '正常', value: '0' }, { label: '停用', value: '1' }]}
        />
        <ProFormText name="remark" label="备注" />
      </ModalForm>
    </PageContainer>
  );
}
