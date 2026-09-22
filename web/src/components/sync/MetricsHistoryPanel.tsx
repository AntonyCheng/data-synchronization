import type { EChartsOption } from 'echarts';
import { ReloadOutlined } from '@ant-design/icons';
import { Button, Descriptions, Empty, Radio, Space, Spin, Tag, Typography } from 'antd';
import ReactECharts from 'echarts-for-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { SyncMetricsSample, SyncMetricsSeries } from '@/api/sync/metrics/types';
import { useAppStore } from '@/stores/appStore';

/**
 * Engine run-metrics history of one task / task-group item: the newest sample as a stat
 * row, then throughput and CDC lag as two single-axis line charts over a trailing window.
 * Re-fetches every 30 s while mounted (the reconcile pass samples at that cadence).
 */
export interface MetricsHistoryPanelProps {
  /** Fetches the series for the given trailing window (minutes). */
  load: (minutes: number) => Promise<SyncMetricsSeries>;
  /** Initial trailing window; default 60 minutes. */
  defaultMinutes?: number;
}

const windowOptions = [
  { label: '15 分钟', value: 15 },
  { label: '1 小时', value: 60 },
  { label: '6 小时', value: 360 },
  { label: '24 小时', value: 1440 },
];

// Categorical slots 1 (source) and 2 (sink) of the validated default palette, in fixed
// order, with their dark-surface steps. Lag is a single series and takes slot 1.
const palette = {
  light: { source: '#2a78d6', sink: '#eb6834', text: '#52514e', muted: '#8a8984', grid: '#e8e7e3', surface: 'transparent' },
  dark: { source: '#3987e5', sink: '#d95926', text: '#c3c2b7', muted: '#8a8984', grid: '#383835', surface: 'transparent' },
};

const numberFormat = new Intl.NumberFormat('zh-CN');

function formatCount(value?: number) {
  return value == null ? '-' : numberFormat.format(value);
}

function formatRate(value?: number) {
  return value == null ? '-' : `${numberFormat.format(Math.round(value * 100) / 100)} 行/秒`;
}

function formatLag(value?: number) {
  if (value == null) return '-';
  if (value < 60) return `${value} 秒`;
  if (value < 3600) return `${Math.floor(value / 60)} 分 ${value % 60} 秒`;
  return `${Math.floor(value / 3600)} 时 ${Math.floor((value % 3600) / 60)} 分`;
}

function timeLabel(sampledAt: string) {
  // Backend serializes LocalDateTime as "yyyy-MM-dd HH:mm:ss"; the chart axis only needs the clock.
  return sampledAt.length >= 19 ? sampledAt.slice(11, 19) : sampledAt;
}

export default function MetricsHistoryPanel({ load, defaultMinutes = 60 }: MetricsHistoryPanelProps) {
  const darkMode = useAppStore(state => state.layoutSettings.darkMode);
  const colors = darkMode ? palette.dark : palette.light;
  const [minutes, setMinutes] = useState(defaultMinutes);
  const [series, setSeries] = useState<SyncMetricsSeries>();
  const [loading, setLoading] = useState(false);
  // Parents pass `load` as an inline arrow; reading it through a ref keeps `refresh` stable so
  // the polling effect below only restarts when the window changes, not on every render.
  const loadRef = useRef(load);
  loadRef.current = load;

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setSeries(await loadRef.current(minutes));
    } finally {
      setLoading(false);
    }
  }, [minutes]);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 30_000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const samples: SyncMetricsSample[] = series?.samples ?? [];
  const latest = series?.latest;

  const baseOption = useMemo<EChartsOption>(() => ({
    backgroundColor: colors.surface,
    textStyle: { color: colors.text },
    grid: { left: 56, right: 16, top: 32, bottom: 28 },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross', lineStyle: { color: colors.muted } },
      backgroundColor: darkMode ? '#1a1a19' : '#fcfcfb',
      borderColor: colors.grid,
      textStyle: { color: colors.text },
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: samples.map(sample => timeLabel(sample.sampledAt)),
      axisLine: { lineStyle: { color: colors.grid } },
      axisLabel: { color: colors.muted, hideOverlap: true },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      min: 0,
      splitLine: { lineStyle: { color: colors.grid } },
      axisLabel: { color: colors.muted },
    },
  }), [colors, darkMode, samples]);

  const throughputOption = useMemo<EChartsOption>(() => ({
    ...baseOption,
    legend: { top: 0, right: 0, textStyle: { color: colors.text }, icon: 'roundRect', itemWidth: 14, itemHeight: 3 },
    yAxis: { ...(baseOption.yAxis as object), name: '行/秒', nameTextStyle: { color: colors.muted, align: 'right' } },
    series: [
      // Sink first (solid), source on top (dashed): when the pipeline is caught up the two
      // rates coincide, and the dash pattern keeps both identities visible.
      { name: '目标提交', type: 'line', showSymbol: false, symbolSize: 8, lineStyle: { width: 2 }, itemStyle: { color: colors.sink }, data: samples.map(sample => sample.sinkQps ?? null), connectNulls: false },
      { name: '源端读取', type: 'line', showSymbol: false, symbolSize: 8, lineStyle: { width: 2, type: 'dashed' }, itemStyle: { color: colors.source }, data: samples.map(sample => sample.sourceQps ?? null), connectNulls: false },
    ],
  }), [baseOption, colors, samples]);

  const backlogOption = useMemo<EChartsOption>(() => ({
    ...baseOption,
    yAxis: { ...(baseOption.yAxis as object), name: '行', minInterval: 1, nameTextStyle: { color: colors.muted, align: 'right' } },
    series: [
      { name: '待提交行数', type: 'line', showSymbol: false, symbolSize: 8, lineStyle: { width: 2 }, itemStyle: { color: colors.source }, areaStyle: { opacity: 0.08 }, data: samples.map(sample => sample.backlogRows ?? null), connectNulls: false },
    ],
  }), [baseOption, colors, samples]);

  const lagOption = useMemo<EChartsOption>(() => ({
    ...baseOption,
    yAxis: { ...(baseOption.yAxis as object), name: '秒', nameTextStyle: { color: colors.muted, align: 'right' } },
    series: [
      { name: '端到端延迟', type: 'line', showSymbol: false, symbolSize: 8, lineStyle: { width: 2 }, itemStyle: { color: colors.source }, areaStyle: { opacity: 0.08 }, data: samples.map(sample => sample.cdcLagSeconds ?? null), connectNulls: false },
    ],
  }), [baseOption, colors, samples]);

  const hasLag = samples.some(sample => sample.cdcLagSeconds != null);

  return (
    <Space orientation="vertical" size={12} style={{ width: '100%' }}>
      <Space wrap style={{ width: '100%', justifyContent: 'space-between' }}>
        <Radio.Group optionType="button" size="small" options={windowOptions} value={minutes} onChange={event => setMinutes(event.target.value)} />
        <Space size={8}>
          <Typography.Text type="secondary">{latest ? `最近采样 ${latest.sampledAt}` : '尚无采样'}</Typography.Text>
          <Button size="small" icon={<ReloadOutlined />} loading={loading} onClick={() => void refresh()}>刷新</Button>
        </Space>
      </Space>
      {latest && (
        <Descriptions size="small" column={{ xs: 1, sm: 2, md: 3 }}>
          <Descriptions.Item label="阶段"><Tag>{latest.phase === 'SNAPSHOT' ? '全量快照' : latest.phase === 'CDC' ? 'CDC 增量' : latest.phase || '-'}</Tag>{latest.engineStatus}</Descriptions.Item>
          <Descriptions.Item label="源端已读取">{formatCount(latest.sourceReceivedCount)} 行</Descriptions.Item>
          <Descriptions.Item label="目标已提交">{formatCount(latest.sinkCommittedCount)} 行</Descriptions.Item>
          <Descriptions.Item label="源端吞吐">{formatRate(latest.sourceQps)}</Descriptions.Item>
          <Descriptions.Item label="目标吞吐">{formatRate(latest.sinkQps)}</Descriptions.Item>
          <Descriptions.Item label="积压">{formatCount(latest.backlogRows)} 行</Descriptions.Item>
          {latest.cdcLagSeconds != null && <Descriptions.Item label="端到端延迟">{formatLag(latest.cdcLagSeconds)}</Descriptions.Item>}
        </Descriptions>
      )}
      <Spin spinning={loading && !series}>
        {samples.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="该时间窗内没有采样；任务运行期间每 30 秒采样一次" />
        ) : (
          <Space orientation="vertical" size={4} style={{ width: '100%' }}>
            <Typography.Text strong>吞吐（行/秒）</Typography.Text>
            <ReactECharts option={throughputOption} notMerge style={{ height: 220 }} />
            <Typography.Text strong>积压（源端已读取 − 目标已提交，行）</Typography.Text>
            <ReactECharts option={backlogOption} notMerge style={{ height: 180 }} />
            {hasLag && <>
              <Typography.Text strong>端到端延迟（秒）</Typography.Text>
              <ReactECharts option={lagOption} notMerge style={{ height: 180 }} />
            </>}
          </Space>
        )}
      </Spin>
    </Space>
  );
}
