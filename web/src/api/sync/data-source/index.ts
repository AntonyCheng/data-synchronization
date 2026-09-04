import type { PageResult, R } from '@/api/types';
import request from '@/api/request';
import type {
  ConnectionTestResult,
  DataSourceCdcPrecheckVO,
  DataSourceCredentialMigrationResult,
  DataSourceForm,
  DataSourceMetadataVO,
  DataSourceQuery,
  DataSourceVO,
  KafkaTopicVO
} from './types';

export function listDataSources(query?: DataSourceQuery) {
  return request<R<PageResult<DataSourceVO>>>({ url: '/sync/data-source/list', method: 'get', params: query });
}

export function getDataSource(id: string | number) {
  return request<R<DataSourceVO>>({ url: `/sync/data-source/${id}`, method: 'get' });
}

export function addDataSource(data: DataSourceForm) {
  return request<R>({ url: '/sync/data-source', method: 'post', data });
}

export function updateDataSource(data: DataSourceForm) {
  return request<R>({ url: '/sync/data-source', method: 'put', data });
}

export function deleteDataSource(id: string | number) {
  return request<R>({ url: `/sync/data-source/${id}`, method: 'delete' });
}

export function testDataSource(id: string | number, data?: DataSourceForm) {
  return request<R<ConnectionTestResult>>({ url: `/sync/data-source/${id}/test`, method: 'post', data });
}

export function listDataSourceDatabases(id: string | number) {
  return request<R<string[]>>({ url: `/sync/data-source/${id}/databases`, method: 'get' });
}

export function listDataSourceTables(id: string | number, databaseName?: string) {
  return request<R<string[]>>({ url: `/sync/data-source/${id}/tables`, method: 'get', params: { databaseName } });
}

export function getDataSourceMetadata(id: string | number, databaseName: string, tableName: string) {
  return request<R<DataSourceMetadataVO>>({
    url: `/sync/data-source/${id}/metadata`,
    method: 'get',
    params: { databaseName, tableName }
  });
}

export function checkDataSourceCdc(id: string | number) {
  return request<R<DataSourceCdcPrecheckVO>>({ url: `/sync/data-source/${id}/cdc-precheck`, method: 'post' });
}

export function migrateDataSourceCredentials() {
  return request<R<DataSourceCredentialMigrationResult>>({ url: '/sync/data-source/credential-migrate', method: 'post' });
}

export function listKafkaTopics(id: string | number) {
  return request<R<KafkaTopicVO[]>>({ url: `/sync/data-source/${id}/kafka/topics`, method: 'get' });
}

export function createKafkaTopic(id: string | number, data: { topic: string; partitions?: number; replicationFactor?: number }) {
  return request<R<KafkaTopicVO>>({ url: `/sync/data-source/${id}/kafka/topics`, method: 'post', data });
}
