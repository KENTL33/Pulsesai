import { useEffect, useRef, useState } from 'react';
import uPlot, { type AlignedData } from 'uplot';
import { useStore } from '../lib/store';

const GRID = '#2e3348';
const TYPE_COLORS: Record<string, string> = {
  temperature: '#f87171',
  humidity:    '#60a5fa',
  pressure:    '#4ade80',
  generic:     '#61dfff',
};



function buildSensorChart(
  container: HTMLDivElement,
  w: number,
  h: number,
  accentColor: string,
  onLegendRef: React.MutableRefObject<(l: { time: string; avg: string } | null) => void>,
): uPlot | null {
  if (w <= 0 || h <= 0) return null;

  const opts: uPlot.Options = {
    width: w,
    height: h,
    scales: { x: { time: true }, y: { auto: true } },
    axes: [
      {
        show: true,
        stroke: '#7a7f99',
        grid: { stroke: GRID, width: 1 },
        ticks: { stroke: GRID },
        font: '10px Fira Code, Consolas, monospace',
        size: 50,
        values: (_u, vals) => vals.map((v) => {
          const d = new Date(v * 1000);
          return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }),
      },
      {
        show: true,
        stroke: '#7a7f99',
        grid: { stroke: GRID, width: 1 },
        ticks: { stroke: GRID },
        font: '10px Fira Code, Consolas, monospace',
        values: (_u, vals) => vals.map((v) => v.toFixed(1)),
      },
    ],
    series: [
      {},
      {
        label: 'Avg',
        stroke: accentColor,
        width: 2,
        fill: `${accentColor}22`,
        points: { show: true, size: 4, stroke: accentColor, fill: accentColor },
      },
    ],
    cursor: { show: true, focus: { proxied: true } },
    legend: { show: false },
    hooks: {
      setCursor: [
        (u: uPlot) => {
          const { left } = u.cursor;
          if (left == null) { onLegendRef.current(null); return; }
          const idx = Math.round(u.posToIdx(left));
          const ts = u.data[0]?.[idx];
          const val = u.data[1]?.[idx];
          if (ts == null || val == null) { onLegendRef.current(null); return; }
          const timeStr = new Date(ts * 1000).toLocaleTimeString('en-US', {
            hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit',
          });
          onLegendRef.current({ time: timeStr, avg: val.toFixed(2) });
        },
      ],
    },
  };

  return new uPlot(opts, [[], []] as AlignedData, container);
}

interface SensorChartProps {
  sensorId: string;
  sensorName: string;
  sensorType: string;
  accentColor: string;
  height?: number;
}

function SensorChart({ sensorId, sensorName, sensorType, accentColor, height = 120 }: SensorChartProps) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const [legend, setLegend] = useState<{ time: string; avg: string } | null>(null);
  const onLegendRef = useRef(setLegend);
  onLegendRef.current = setLegend;

  const metrics = useStore((s) => s.metricsBySensor[sensorId] ?? []);

  useEffect(() => {
    if (!wrapRef.current) return;
    const el = wrapRef.current;
    const parent = el.parentElement!;

    let rafId: number;

    function tryBuild() {
      const w = el.clientWidth;
      if (w === 0) {
        rafId = requestAnimationFrame(tryBuild);
        return;
      }
        if (!chartRef.current) {
          chartRef.current = buildSensorChart(el, w, height, accentColor, onLegendRef);
        }
    }

    rafId = requestAnimationFrame(tryBuild);

    const ro = new ResizeObserver((entries) => {
      const newW = entries[0].contentRect.width;
      if (newW === 0) return;

      if (!chartRef.current) {
        chartRef.current = buildSensorChart(el, newW, height, accentColor, onLegendRef);
        return;
      }

      const old = chartRef.current;
      chartRef.current = buildSensorChart(el, newW, height, accentColor, onLegendRef);
      old.destroy();
    });

    ro.observe(parent);

    return () => {
      cancelAnimationFrame(rafId);
      ro.disconnect();
      chartRef.current?.destroy();
      chartRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [accentColor, height, sensorId]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    pushData(metrics, chart);
    setChartScale(chart, metrics);
  }, [metrics]);

  const latest = metrics.length > 0 ? metrics[metrics.length - 1] : undefined;

  return (
    <div className="overview-card">
      <div className="overview-card-header">
        <span className="overview-name">{sensorName}</span>
        <span className="overview-type">{sensorType}</span>
        {latest && (
          <span className="overview-val" style={{ color: accentColor }}>
            {latest.avgValue.toFixed(2)}
          </span>
        )}
      </div>
      <div className="overview-legend">
        {legend && (
          <>
            <span>Time: {legend.time}</span>
            <span>Avg: <span style={{ color: accentColor }}>{legend.avg}</span></span>
          </>
        )}
      </div>
      <div className="overview-chart" ref={wrapRef}>
        {metrics.length === 0 && (
          <div className="overview-empty">no data</div>
        )}
      </div>
    </div>
  );
}

function pushData(
  metrics: { windowStart: string; avgValue: number; minValue: number; maxValue: number }[],
  chart: uPlot,
) {
  if (metrics.length === 0) return;
  const cutoff = Date.now() - 5 * 60 * 1000;
  const visible = metrics
    .map((m) => ({ ...m, tsMs: new Date(m.windowStart).getTime() }))
    .filter((m) =>
      m.tsMs >= cutoff
      && Number.isFinite(m.avgValue)
      && Number.isFinite(m.minValue)
      && Number.isFinite(m.maxValue),
    )
    .sort((a, b) => a.tsMs - b.tsMs);
  if (visible.length === 0) return;

  const chartData: AlignedData = [
    visible.map((m) => m.tsMs / 1000),
    visible.map((m) => m.avgValue),
  ];
  chart.setData(chartData);
}

function setChartScale(chart: uPlot, metrics: { minValue: number; maxValue: number }[]) {
  if (metrics.length === 0) return;
  const values = metrics.map((m) => m.minValue).concat(metrics.map((m) => m.maxValue)).filter(Number.isFinite);
  if (values.length === 0) return;
  const low = Math.min(...values);
  const high = Math.max(...values);
  const span = Math.max(high - low, Math.max(Math.abs(high), Math.abs(low)) * 0.05, 1);
  const pad = span * 0.15;
  const MIN_RANGE = 5;
  const range = Math.max(high - low, MIN_RANGE);
  chart.setScale('y', {
    min: low - pad,
    max: low + range + pad,
  });
}

export function AllSensorsOverview() {
  const sensors = useStore((s) => s.sensors);

  if (sensors.length === 0) {
    return (
      <div className="overview-grid">
        <div className="overview-empty-msg">No sensors registered</div>
      </div>
    );
  }

  return (
    <div className="overview-grid">
      {sensors.map((s) => (
        <SensorChart
          key={s.id}
          sensorId={s.id}
          sensorName={s.name}
          sensorType={s.type}
          accentColor={TYPE_COLORS[s.type.toLowerCase()] ?? TYPE_COLORS.generic}
        />
      ))}
    </div>
  );
}