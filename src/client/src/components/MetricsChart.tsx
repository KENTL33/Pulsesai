import { useEffect, useRef } from 'react';
import uPlot, { type AlignedData } from 'uplot';
import { useChartData } from '../lib/useChartData';

const GRID = '#2e3348';
const AVG_C = '#61dfff';
const MINMAX_C = 'rgba(97,223,255,0.25)';

function buildChart(container: HTMLDivElement, w: number, h: number): uPlot | null {
  if (w <= 0 || h <= 0) return null;
  const opts: uPlot.Options = {
    width: w,
    height: h,
    scales: { x: { time: true }, y: { auto: true } },
    axes: [
      {
        stroke: '#7a7f99',
        grid: { stroke: GRID, width: 1 },
        ticks: { stroke: GRID },
        font: '11px Fira Code, Consolas, monospace',
        values: (_u, vals) => vals.map((v) => {
          const d = new Date(v * 1000);
          return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }),
      },
      {
        stroke: '#7a7f99',
        grid: { stroke: GRID, width: 1 },
        ticks: { stroke: GRID },
        font: '11px Fira Code, Consolas, monospace',
        values: (_u, vals) => vals.map((v) => v.toFixed(2)),
      },
    ],
    series: [
      {},
      { label: 'Avg', stroke: AVG_C, width: 2 },
      { label: 'Min', stroke: MINMAX_C, width: 1 },
      { label: 'Max', stroke: MINMAX_C, width: 1 },
    ],
  };
  return new uPlot(opts, [[], [], [], []] as AlignedData, container);
}

export function MetricsChart() {
  const wrapRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  // Let the hook read the selected sensor from the store itself
  // (avoid passing potentially-null/undefined values which can change
  // how the hook resolves the effective sensor id)
  const data = useChartData();

  // Init effect — one-time chart build
  useEffect(() => {
    if (!wrapRef.current) return;
    const el = wrapRef.current;
    const parent = el.parentElement;
    if (!parent) return;

    let rafId: number;

    function tryBuild() {
      const w = el.clientWidth;
      const h = el.clientHeight;
      if (w === 0 || h === 0) {
        rafId = requestAnimationFrame(tryBuild);
        return;
      }
      if (!chartRef.current) {
        chartRef.current = buildChart(el, w, h);
      }
    }

    rafId = requestAnimationFrame(tryBuild);

    const ro = new ResizeObserver((entries) => {
      const w = entries[0].contentRect.width;
      const h = entries[0].contentRect.height;
      if (w === 0 || h === 0) return;

      if (!chartRef.current) {
        chartRef.current = buildChart(el, w, h);
        return;
      }

      const old = chartRef.current;
      chartRef.current = buildChart(el, w, h);
      old.destroy();
    });

    ro.observe(parent);

    return () => {
      cancelAnimationFrame(rafId);
      ro.disconnect();
      chartRef.current?.destroy();
      chartRef.current = null;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Data effect — push data whenever it changes (chart built with auto Y scale)
  useEffect(() => {
    console.log('[MetricsChart] data effect', { hasData: !!data, count: data?.count });
    const chart = chartRef.current;
    if (!chart || !data) return;
    const chartData: AlignedData = [
      data.timestamps,
      data.avgValues,
      data.minValues,
      data.maxValues,
    ];
    chart.setData(chartData);

    const low = Math.min(...data.minValues);
    const high = Math.max(...data.maxValues);
    const span = Math.max(high - low, Math.max(Math.abs(high), Math.abs(low)) * 0.05, 1);
    const pad = span * 0.15;
    chart.setScale('y', {
      min: low - pad,
      max: high + pad,
    });
  }, [data]);

  return (
    <div className="uplot-wrap" ref={wrapRef}>
      {!data && <div className="empty-state">Select a sensor to view metrics</div>}
    </div>
  );
}