import { useEffect, useRef } from 'react';
import uPlot, { type AlignedData } from 'uplot';
import { useChartData } from '../lib/useChartData';

const GRID = '#2e3348';
const AVG_C = '#61dfff';

interface Props {
  sensorId: string | null | undefined;
  height?: number;
  label?: string;
  accentColor?: string;
}

function buildMini(
  container: HTMLDivElement,
  w: number,
  h: number,
  accentColor: string,
  label?: string,
): uPlot | null {
  if (w <= 0 || h <= 0) return null;

  const opts: uPlot.Options = {
    width: w,
    height: h,
    tzDate: (ts) => uPlot.tzDate(new Date(ts * 1000), 'UTC'),
    scales: { x: { time: true }, y: { auto: true } },
    axes: [
      { show: false },
      { show: true, stroke: '#7a7f99', font: '9px Fira Code, Consolas, monospace', values: (_u, vals) => vals.map((v) => v.toFixed(1)) },
    ],
    series: [
      {},
      { label, stroke: accentColor, width: 1.5, fill: `${accentColor}22` },
    ],
    plugins: [
      {
        hooks: {
          draw: [
            (u: uPlot) => {
              if (u.scales.y.min! <= 0 && u.scales.y.max! >= 0) {
                const y = u.valToPos(0, 'y', true);
                u.ctx.strokeStyle = GRID;
                u.ctx.lineWidth = 0.5;
                u.ctx.beginPath();
                u.ctx.moveTo(u.bbox.left, y);
                u.ctx.lineTo(u.bbox.left + u.bbox.width, y);
                u.ctx.stroke();
              }
            },
          ],
        },
      },
    ],
  };

  return new uPlot(opts, [[], []] as AlignedData, container);
}

export function MiniChart({ sensorId, height = 60, label, accentColor = AVG_C }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const data = useChartData(sensorId);

  useEffect(() => {
    if (!wrapRef.current) return;
    const el = wrapRef.current;

    let rafId: number;

    function tryBuild() {
      const w = el.clientWidth;
      if (w === 0 || height === 0) {
        rafId = requestAnimationFrame(tryBuild);
        return;
      }
      if (!chartRef.current) {
        chartRef.current = buildMini(el, w, height, accentColor, label);
      }
    }

    rafId = requestAnimationFrame(tryBuild);

    const ro = new ResizeObserver((entries) => {
      const newW = entries[0].contentRect.width;
      if (newW === 0 || height === 0) return;

      if (!chartRef.current) {
        chartRef.current = buildMini(el, newW, height, accentColor, label);
        return;
      }

      const old = chartRef.current;
      chartRef.current = buildMini(el, newW, height, accentColor, label);
      old.destroy();

      if (data) {
        chartRef.current?.setData([data.timestamps, data.avgValues] as AlignedData);
      }
    });

    ro.observe(el);

    return () => {
      cancelAnimationFrame(rafId);
      ro.disconnect();
      chartRef.current?.destroy();
      chartRef.current = null;
    };
  }, [height, label, accentColor]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!chartRef.current || !data) return;
    chartRef.current.setData([data.timestamps, data.avgValues] as AlignedData);
  }, [data]);

  if (!data || data.count === 0) return null;

  return <div ref={wrapRef} style={{ width: '100%', height: `${height}px` }} />;
}