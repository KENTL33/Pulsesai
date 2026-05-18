import { useEffect, useRef } from 'react';
import * as signalR from '@microsoft/signalr';
import { useStore } from './store';
import type { AggregatedMetric, Alert } from '../types';

const HUB_URL = import.meta.env.VITE_SIGNALR_URL || '/hubs/analytics';

export function useSignalR() {
  const setConnectionState = useStore((s) => s.setConnectionState);
  const connectionState = useStore((s) => s.connectionState);
  const addMetric = useStore((s) => s.addMetric);
  const addAlert = useStore((s) => s.addAlert);
  const selectedSensorId = useStore((s) => s.selectedSensorId);
  const connRef = useRef<signalR.HubConnection | null>(null);

  useEffect(() => {
    const conn = new signalR.HubConnectionBuilder()
      .withUrl(HUB_URL)
      .withAutomaticReconnect([0, 2000, 5000, 10000, 30000])
      .configureLogging(signalR.LogLevel.Warning)
      .build();

    connRef.current = conn;

    conn.on('MetricReceived', (metric: AggregatedMetric) => {
      addMetric(metric);
    });

    conn.on('MetricBatchReceived', (metrics: AggregatedMetric[]) => {
      metrics.forEach((metric) => addMetric(metric));
    });

    conn.on('AlertTriggered', (alert: Alert) => {
      addAlert(alert);
    });

    conn.onreconnecting(() => setConnectionState('connecting'));
    conn.onreconnected(() => setConnectionState('connected'));
    conn.onclose(() => setConnectionState('disconnected'));

    setConnectionState('connecting');
    conn.start().then(() => setConnectionState('connected')).catch(() => setConnectionState('disconnected'));

    return () => {
      conn.stop().catch(() => {});
    };
  }, [addMetric, addAlert, setConnectionState]);

  // Subscribe to sensor groups when a sensor is selected
  useEffect(() => {
    const conn = connRef.current;
    if (!conn || conn.state !== signalR.HubConnectionState.Connected) return;

    if (selectedSensorId) {
      conn.invoke('subscribeSensor', selectedSensorId).catch(() => {});
    } else {
      conn.invoke('subscribeAll').catch(() => {});
    }
  }, [selectedSensorId, connectionState]);
}