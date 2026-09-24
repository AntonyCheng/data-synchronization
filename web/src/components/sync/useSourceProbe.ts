import { useState } from 'react';
import type { ConnectionTestResult, DataSourceCdcPrecheckVO, DataSourceOptionVO } from '@/api/sync/data-source/types';
import {
  checkDataSourceCdc,
  listDataSourceDatabases,
  listDataSourceTables,
  listKafkaTopics,
  testDataSource
} from '@/api/sync/data-source';

/** Hands a probe continuation the database it listed and a way to keep reporting progress. */
export interface SourceProbeContext {
  sourceId: string | number;
  database: string;
  setPhase: (phase: string) => void;
}

/**
 * What both creation wizards learn about a MySQL source before the operator may continue: a
 * fresh connection test, the binlog / CDC precheck, its databases and the tables of the default
 * database. It reconnects every time rather than trusting the stored connection state.
 *
 * `phase` says what the probe is doing right now. It is shown while the probe runs, because the
 * probe takes a couple of seconds and used to be completely silent - the only feedback was an
 * error toast if you clicked 下一步 too early.
 */
export function useSourceProbe(dataSources: DataSourceOptionVO[]) {
  const [sourceDatabases, setSourceDatabases] = useState<string[]>([]);
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [sourceDatabase, setSourceDatabase] = useState('');
  const [cdcPrecheck, setCdcPrecheck] = useState<DataSourceCdcPrecheckVO>();
  const [connectionTest, setConnectionTest] = useState<ConnectionTestResult>();
  const [loading, setLoading] = useState(false);
  const [phase, setPhase] = useState<string>();

  const reset = () => {
    setSourceDatabases([]);
    setSourceTables([]);
    setSourceDatabase('');
    setCdcPrecheck(undefined);
    setConnectionTest(undefined);
  };

  /** Runs one piece of introspection with the busy flag set and its progress on the banner. */
  const track = async (firstPhase: string, work: (setPhase: (phase: string) => void) => Promise<void>) => {
    setLoading(true);
    setPhase(firstPhase);
    try {
      await work(setPhase);
    } finally {
      setLoading(false);
      setPhase(undefined);
    }
  };

  /**
   * Connection test -> CDC precheck -> databases -> tables of the default database. `then`
   * continues inside the same busy window once the tables are listed, so a wizard that also
   * needs one table's structure does not flash the banner off and on again.
   */
  const probe = async (sourceId?: string | number, then?: (context: SourceProbeContext) => Promise<void>) => {
    reset();
    if (!sourceId) return;
    const source = dataSources.find(item => String(item.sourceId) === String(sourceId));
    if (!source) return;
    await track('正在测试源端连接…', async setPhase => {
      const connectionResult = await testDataSource(sourceId);
      setConnectionTest(connectionResult.data);
      if (!connectionResult.data?.success) return;
      if (source.sourceType === 'MYSQL') {
        setPhase('正在检查 binlog / CDC 前置条件…');
        const cdcResult = await checkDataSourceCdc(sourceId);
        setCdcPrecheck(cdcResult.data);
      }
      setPhase('正在读取数据库列表…');
      const databasesResult = await listDataSourceDatabases(sourceId);
      const databases = databasesResult.data || [];
      setSourceDatabases(databases);
      const database = source.databaseName || databases[0] || '';
      setSourceDatabase(database);
      if (!database) return;
      setPhase('正在读取表列表…');
      const tablesResult = await listDataSourceTables(sourceId, database);
      setSourceTables(tablesResult.data || []);
      if (then) await then({ sourceId, database, setPhase });
    });
  };

  return {
    sourceDatabases,
    sourceTables,
    sourceDatabase,
    cdcPrecheck,
    connectionTest,
    loading,
    phase,
    probe,
    track,
    reset
  };
}

export type SourceProbe = ReturnType<typeof useSourceProbe>;

/** Tables of a relational target, or topics of a Kafka one, offered by the target-name pickers. */
export function useTargetObjects(dataSources: DataSourceOptionVO[]) {
  const [names, setNames] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);

  const load = async (targetId?: string | number) => {
    setNames([]);
    if (!targetId) return;
    const target = dataSources.find(item => String(item.sourceId) === String(targetId));
    if (!target) return;
    setLoading(true);
    try {
      if (target.sourceType === 'KAFKA') {
        const result = await listKafkaTopics(targetId);
        setNames((result.data || []).map(item => item.topic));
      } else {
        const result = await listDataSourceTables(targetId, target.databaseName);
        setNames(result.data || []);
      }
    } finally {
      setLoading(false);
    }
  };

  const reset = () => {
    setNames([]);
    setLoading(false);
  };

  return { names, loading, load, reset };
}
