import { create } from 'zustand';
import type { Sensor, Alert, AggregatedMetric } from '../types';

const MAX_POINTS = 300;

interface MetricsState {
  sensors: Sensor[];
  selectedSensorId: string | null;
  alerts: Alert[];
  metricsBySensor: Record<string, AggregatedMetric[]>;
  connectionState: 'connected' | 'connecting' | 'disconnected';

  setSensors: (sensors: Sensor[]) => void;
  selectSensor: (id: string | null) => void;
  addAlert: (alert: Alert) => void;
  addMetric: (metric: AggregatedMetric) => void;
  setConnectionState: (state: 'connected' | 'connecting' | 'disconnected') => void;
}

export const useStore = create<MetricsState>((set) => ({
  sensors: [],
  selectedSensorId: null,
  alerts: [],
  metricsBySensor: {},
  connectionState: 'disconnected',

  setSensors: (sensors) => set({ sensors }),

  selectSensor: (id) => {
    const prev = useStore.getState().selectedSensorId;
    console.log('[store] selectSensor', { prev, next: id });
    set({ selectedSensorId: id });
  },

  addAlert: (alert) => set((s) => ({
    alerts: [alert, ...s.alerts].slice(0, 100),
  })),

  addMetric: (metric) => set((s) => {
    if (metric.count <= 0) {
      return s;
    }

    const key = metric.sensorId;
    const existing = s.metricsBySensor[key] ?? [];

    // Work on a shallow copy
    const next = [...existing];
    const metricTs = new Date(metric.windowStart).getTime();

    // Replace existing entry for the same window if present
    const existingIdx = next.findIndex((m) => new Date(m.windowStart).getTime() === metricTs);
    if (existingIdx !== -1) {
      next[existingIdx] = metric;
    } else {
      // Insert in chronological order to keep arrays sorted
      const insertAt = next.findIndex((m) => new Date(m.windowStart).getTime() > metricTs);
      if (insertAt === -1) next.push(metric);
      else next.splice(insertAt, 0, metric);

      // Trim to MAX_POINTS keeping the most recent points
      if (next.length > MAX_POINTS) next.splice(0, next.length - MAX_POINTS);
    }

    return {
      metricsBySensor: { ...s.metricsBySensor, [key]: next },
    };
  }),

  setConnectionState: (connectionState) => set({ connectionState }),
}));