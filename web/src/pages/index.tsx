import type { EChartsOption } from 'echarts';
import {
  ApiOutlined,
  ArrowRightOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
  DatabaseOutlined,
  DeploymentUnitOutlined,
  PlusOutlined,
  ReloadOutlined,
  ThunderboltOutlined,
  WarningOutlined
} from '@ant-design/icons';
import { PageContainer } from '@ant-design/pro-components';
import { history } from '@umijs/max';
import { Alert, Button, Card, Col, Empty, Progress, Row, Spin, Statistic, Tag, Typography } from 'antd';
import ReactECharts from 'echarts-for-react';
import { useEffect, useMemo, useState } from 'react';
import type { SyncOverviewVO } from '@/api/sync/overview/types';
import type { SyncTaskVO } from '@/api/sync/task/types';
import { getSyncOverview } from '@/api/sync/overview';
import { listSyncTasks } from '@/api/sync/task';

const statusLabels: Record<string, string> = {
  RUNNING: '运行中',
  PAUSING: '暂停中',
  PAUSED: '已暂停',
  FAILED: '失败',
  FINISHED: '已完成',
  STOPPED: '已停止',
  DRAFT: '草稿',
  REINITIALIZE_REQUIRED: '需重新初始化'
};
const modeLabels: Record<string, string> = { FULL: '全量', INCREMENTAL: '增量', FULL_CDC: '全量 + CDC' };
const typeLabels: Record<string, string> = { MYSQL: 'MySQL', POSTGRESQL: 'PostgreSQL', KAFKA: 'Kafka' };

function statusColor(status?: string) {
  if (status === 'RUNNING') return 'processing';
  if (status === 'FINISHED') return 'success';
  if (status === 'FAILED' || status === 'REINITIALIZE_REQUIRED') return 'error';
  if (status === 'PAUSED' || status === 'PAUSING') return 'warning';
  return 'default';
}
function formatNumber(value: number) {
  return new Intl.NumberFormat('zh-CN').format(value);
}
function formatTime(value?: string) {
  return value ? value.replace('T', ' ').slice(0, 19) : '暂无记录';
}

export default function Dashboard() {
  const [overview, setOverview] = useState<SyncOverviewVO>();
  const [recentTasks, setRecentTasks] = useState<SyncTaskVO[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string>();
  const [refreshedAt, setRefreshedAt] = useState<Date>(new Date());

  const loadSnapshot = async () => {
    setLoading(true);
    setLoadError(undefined);
    // Counters come from the server so they cover task groups too and are not capped at a page;
    // the list below is genuinely just the newest handful.
    const [overviewResult, taskResult] = await Promise.allSettled([
      getSyncOverview(),
      listSyncTasks({ pageNum: 1, pageSize: 6 })
    ]);
    if (overviewResult.status === 'fulfilled') setOverview(overviewResult.value.data);
    if (taskResult.status === 'fulfilled') setRecentTasks(taskResult.value.data?.rows || []);
    if (overviewResult.status === 'rejected' || taskResult.status === 'rejected')
      setLoadError('部分运营数据暂时无法读取，请检查后端服务或刷新重试。');
    setRefreshedAt(new Date());
    setLoading(false);
  };
  useEffect(() => {
    void loadSnapshot();
    // Keep the console current without the operator having to click 刷新.
    const timer = window.setInterval(() => void loadSnapshot(), 30000);
    return () => window.clearInterval(timer);
  }, []);

  const runningJobs = overview?.jobRunning ?? 0;
  const totalJobs = overview?.jobTotal ?? 0;
  const activeSources = overview?.dataSourceEnabled ?? 0;
  const kafkaJobs = overview?.targetTypeCounts?.KAFKA ?? 0;
  const checkedRows = overview?.checkedRows ?? 0;
  const avgLag = overview?.avgCdcLagSeconds ?? 0;
  const successRate = overview?.checkedJobs ? Math.round((overview.checkedMatched / overview.checkedJobs) * 100) : 0;

  const statusChart = useMemo<EChartsOption>(() => {
    const counts = Object.entries(statusLabels)
      .map(([value, name]) => ({ value: overview?.statusCounts?.[value] ?? 0, name }))
      .filter(item => item.value > 0);
    return {
      tooltip: { trigger: 'item' },
      legend: { bottom: 0, textStyle: { color: '#93a4b8' } },
      series: [
        {
          type: 'pie',
          radius: ['54%', '78%'],
          center: ['50%', '43%'],
          avoidLabelOverlap: true,
          itemStyle: { borderColor: '#111b2e', borderWidth: 3 },
          label: { show: false },
          data: counts.length ? counts : [{ value: 1, name: '暂无任务', itemStyle: { color: '#334155' } }]
        }
      ],
      color: ['#21d4a7', '#4ea1ff', '#f7b955', '#ff6b6b', '#8b9bb4', '#7c8cff', '#45c7d9', '#aab4c3']
    };
  }, [overview]);
  const chainChart = useMemo<EChartsOption>(() => {
    const chainTypes = ['POSTGRESQL', 'MYSQL', 'KAFKA'];
    const labels = chainTypes.map(type => `MySQL → ${typeLabels[type]}`);
    const values = chainTypes.map(type => overview?.targetTypeCounts?.[type] ?? 0);
    return {
      grid: { left: 96, right: 20, top: 12, bottom: 20 },
      xAxis: {
        type: 'value',
        minInterval: 1,
        splitLine: { lineStyle: { color: '#24334b' } },
        axisLabel: { color: '#8fa1b8' }
      },
      yAxis: { type: 'category', data: labels, axisLabel: { color: '#c3d0df' } },
      tooltip: { trigger: 'axis' },
      series: [{ type: 'bar', data: values, barWidth: 14, itemStyle: { color: '#3ed6c0', borderRadius: [0, 4, 4, 0] } }]
    };
  }, [overview]);

  return (
    <PageContainer title={false} className="dashboard-page">
      <div className="home-dashboard">
        <section className="dashboard-status-band">
          <div>
            <div className="dashboard-eyebrow">
              <span className="status-dot" /> 实时计算控制台
            </div>
            <Typography.Title level={2}>一站式实时计算平台</Typography.Title>
            <Typography.Paragraph>统一管理数据源、同步任务、实时链路与运行质量。</Typography.Paragraph>
          </div>
          <div className="dashboard-status-actions">
            <Tag color="success" icon={<CheckCircleOutlined />}>
              平台在线
            </Tag>
            <span>
              快照于{' '}
              {refreshedAt.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
            </span>
            <Button type="text" icon={<ReloadOutlined />} onClick={() => void loadSnapshot()} loading={loading}>
              刷新
            </Button>
          </div>
        </section>
        {loadError && (
          <Alert type="warning" showIcon message={loadError} closable onClose={() => setLoadError(undefined)} />
        )}
        <Row gutter={[16, 16]} className="dashboard-kpis">
          <Col xs={24} sm={12} xl={6}>
            <Card className="kpi-card kpi-green">
              <Statistic
                title="运行中作业"
                value={runningJobs}
                suffix={`/ ${totalJobs}`}
                prefix={<ThunderboltOutlined />}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} xl={6}>
            <Card className="kpi-card kpi-blue">
              <Statistic title="活跃数据源" value={activeSources} prefix={<DatabaseOutlined />} />
            </Card>
          </Col>
          <Col xs={24} sm={12} xl={6}>
            <Card className="kpi-card kpi-orange">
              <Statistic
                title="已核对数据行"
                value={checkedRows}
                formatter={value => formatNumber(Number(value))}
                prefix={<DeploymentUnitOutlined />}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} xl={6}>
            <Card className="kpi-card kpi-cyan">
              <Statistic title="平均 CDC 延迟" value={avgLag} suffix="秒" prefix={<ClockCircleOutlined />} />
            </Card>
          </Col>
        </Row>
        <Row gutter={[16, 16]}>
          <Col xs={24} xl={16}>
            <Card
              title="同步链路概览"
              extra={
                <Button type="link" onClick={() => history.push('/sync/task')}>
                  查看任务 <ArrowRightOutlined />
                </Button>
              }
              className="dashboard-card"
            >
              <div className="chain-grid">
                {['POSTGRESQL', 'MYSQL', 'KAFKA'].map(type => {
                  const count = overview?.targetTypeCounts?.[type] ?? 0;
                  const active = overview?.targetTypeRunningCounts?.[type] ?? 0;
                  return (
                    <div className="chain-item" key={type}>
                      <div className="chain-icon">
                        <ApiOutlined />
                      </div>
                      <div className="chain-main">
                        <strong>
                          MySQL <ArrowRightOutlined /> {typeLabels[type]}
                        </strong>
                        <span>
                          {count} 个任务，{active} 个运行中
                        </span>
                      </div>
                      <Tag color={active ? 'success' : count ? 'processing' : 'default'}>
                        {active ? '链路活跃' : count ? '已配置' : '待配置'}
                      </Tag>
                    </div>
                  );
                })}
              </div>
              <div className="chart-wrap">
                <ReactECharts option={chainChart} style={{ height: 190 }} notMerge />
              </div>
            </Card>
          </Col>
          <Col xs={24} xl={8}>
            <Card title="任务状态分布" className="dashboard-card dashboard-chart-card">
              <ReactECharts option={statusChart} style={{ height: 260 }} notMerge />
              <div className="chart-summary">
                <span>
                  <b>{successRate}%</b> 核对一致率
                </span>
                <span>
                  <b>{kafkaJobs}</b> Kafka 作业
                </span>
              </div>
            </Card>
          </Col>
        </Row>
        <Row gutter={[16, 16]}>
          <Col xs={24} xl={16}>
            <Card
              title="最近任务"
              className="dashboard-card"
              extra={
                <Button type="link" onClick={() => history.push('/sync/task')}>
                  全部任务 <ArrowRightOutlined />
                </Button>
              }
            >
              {loading ? (
                <div className="dashboard-loading">
                  <Spin />
                </div>
              ) : recentTasks.length ? (
                <div className="recent-task-list">
                  {recentTasks.map(task => (
                    <div className="recent-task-row" key={String(task.taskId)}>
                      <div className="task-name">
                        <strong>{task.taskName || `任务 ${task.taskId}`}</strong>
                        <span>
                          {task.sourceTable || '-'} → {task.targetTable || '-'}
                        </span>
                      </div>
                      <Tag color={statusColor(task.status)}>
                        {statusLabels[task.status || ''] || task.status || '未知'}
                      </Tag>
                      <span className="task-mode">{modeLabels[task.syncMode || ''] || task.syncMode || '-'}</span>
                      <span className="task-time">{formatTime(task.updateTime || task.createTime)}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无同步任务" />
              )}
            </Card>
          </Col>
          <Col xs={24} xl={8}>
            <Card title="平台健康度" className="dashboard-card health-card">
              <div className="health-score">
                <div>
                  <span>服务健康度</span>
                  <strong>{loadError ? '需关注' : '良好'}</strong>
                </div>
                <Progress
                  type="circle"
                  percent={loadError ? 72 : 100}
                  size={82}
                  strokeColor={loadError ? '#f7b955' : '#21d4a7'}
                />
              </div>
              <div className="health-list">
                <div>
                  <span className="health-state-success">
                    <CheckCircleOutlined /> 后端 API
                  </span>
                  <Tag color="success">正常</Tag>
                </div>
                <div>
                  <span className="health-state-success">
                    <CheckCircleOutlined /> 数据源连接
                  </span>
                  <Tag color={overview?.dataSourceTotal ? 'success' : 'default'}>
                    {overview?.dataSourceTotal ? '已接入' : '暂无'}
                  </Tag>
                </div>
                <div>
                  <span className={kafkaJobs ? 'health-state-success' : 'health-state-warning'}>
                    {kafkaJobs ? <CheckCircleOutlined /> : <WarningOutlined />} Kafka 目标链路
                  </span>
                  <Tag color={kafkaJobs ? 'success' : 'warning'}>{kafkaJobs ? '已配置' : '待配置'}</Tag>
                </div>
              </div>
            </Card>
          </Col>
        </Row>
        <div className="dashboard-quick-actions">
          <Button type="primary" icon={<PlusOutlined />} onClick={() => history.push('/sync/task')}>
            新建同步任务
          </Button>
          <Button icon={<DatabaseOutlined />} onClick={() => history.push('/sync/data-source')}>
            管理数据源
          </Button>
          <Button icon={<ApiOutlined />} onClick={() => history.push('/sync/group')}>
            多表同步
          </Button>
        </div>
      </div>
    </PageContainer>
  );
}
