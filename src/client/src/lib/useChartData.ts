import { useMemo } from 'react';
import { useStore } from './store';

const WINDOW_MS = 5 * 60 * 1000; // 5-minute visible window

export function useChartData(sensorId?: string | null) {
  // Use provided sensorId, or fall back to store's selectedSensorId
  const storeSensorId = useStore((s) => s.selectedSensorId);
  const effectiveId = sensorId !== undefined ? sensorId : storeSensorId;

  // sensors list not needed here

  const metrics = useStore((s) => {
    if (effectiveId) {
      return s.metricsBySensor[effectiveId] ?? [];
    }
    return Object.values(s.metricsBySensor).flat();
  });

  return useMemo(() => {
    if (metrics.length === 0) {
      console.log('[useChartData] no metrics for effectiveId:', effectiveId);
      return null;
    }

    console.log('[useChartData] got', metrics.length, 'metrics for sensor:', effectiveId);

    const now = Date.now();
    const cutoff = now - WINDOW_MS;
    const visible = metrics.filter((m) => {
      const ts = new Date(m.windowStart).getTime();
      return ts >= cutoff;
    });

    // Ensure chronological order (uPlot requires monotonic increasing x values)
    visible.sort((a, b) => new Date(a.windowStart).getTime() - new Date(b.windowStart).getTime());

    // Preserve millisecond precision so 100ms windows render as distinct points
    const timestamps = visible.map((m) => new Date(m.windowStart).getTime() / 1000);
    const avgValues  = visible.map((m) => m.avgValue);
    const minValues  = visible.map((m) => m.minValue);
    const maxValues  = visible.map((m) => m.maxValue);
    return { timestamps, avgValues, minValues, maxValues, count: visible.length };
  }, [metrics]);
}
