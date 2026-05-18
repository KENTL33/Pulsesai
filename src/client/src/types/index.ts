export interface Sensor {
  id: string;
  name: string;
  type: string;
  unit?: string;
  location?: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface Alert {
  id: string;
  sensorId: string;
  ruleId: string;
  severity: 'info' | 'warning' | 'critical';
  message: string;
  valueAtTrigger: number;
  thresholdValue: number;
  status: 'active' | 'acknowledged' | 'resolved';
  triggeredAt: string;
  acknowledgedAt?: string;
  resolvedAt?: string;
}

export interface AggregatedMetric {
  sensorId: string;
  windowStart: string;
  windowDurationMs: number;
  avgValue: number;
  minValue: number;
  maxValue: number;
  count: number;
  stdDev: number;
}