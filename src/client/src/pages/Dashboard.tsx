import { useEffect } from 'react';
import { useStore } from '../lib/store';
import { SensorCard } from '../components/SensorCard';
import { MiniChart } from '../components/MiniChart';
import { MetricsChart } from '../components/MetricsChart';
import { AllSensorsOverview } from '../components/AllSensorsOverview';
import { AlertsPanel } from '../components/AlertsPanel';
import type { Sensor } from '../types';

const TYPE_COLORS: Record<string, string> = {
  temperature: '#f87171',
  humidity:    '#60a5fa',
  pressure:    '#4ade80',
  generic:     '#61dfff',
};

export function Dashboard() {
  const sensors = useStore((s) => s.sensors);
  const selectedSensorId = useStore((s) => s.selectedSensorId);
  const metricsBySensor = useStore((s) => s.metricsBySensor);
  const setSensors = useStore((s) => s.setSensors);

  useEffect(() => {
    fetch('/api/sensors')
      .then((r) => r.json())
      .then((data: unknown) => setSensors((data as Sensor[]) ?? []))
      .catch(() => {});
  }, [setSensors]);

  const selected = sensors.find((s) => s.id === selectedSensorId);
  const latestMetric = selectedSensorId
    ? (metricsBySensor[selectedSensorId] ?? []).slice(-1)[0]
    : undefined;

  return (
    <div className="layout">
      <aside className="sidebar">
        <SensorCard
          sensor={{ id: '__all__', name: 'All Sensors', type: 'virtual', unit: '', isActive: true, createdAt: '', updatedAt: '' }}
          isSelected={!selectedSensorId}
        />
        {sensors.map((s) => (
          <SensorCardWithMini
            key={s.id}
            sensor={s}
            isSelected={s.id === selectedSensorId}
            latestMetric={
              s.id === selectedSensorId && latestMetric
                ? { avgValue: latestMetric.avgValue, minValue: latestMetric.minValue, maxValue: latestMetric.maxValue }
                : undefined
            }
          />
        ))}
      </aside>

      <section className="main-area">
        {selectedSensorId ? (
          <div className="chart-area">
            <div className="chart-header">
              <h2>{selected?.name ?? 'Sensor'}</h2>
              {selected && <span>{selected.unit}</span>}
            </div>
            <MetricsChart />
          </div>
        ) : (
          <AllSensorsOverview />
        )}
      </section>

      <AlertsPanel />
    </div>
  );
}

function SensorCardWithMini({
  sensor,
  isSelected,
  latestMetric,
}: {
  sensor: Sensor;
  isSelected: boolean;
  latestMetric?: { avgValue: number; minValue: number; maxValue: number };
}) {
  const selectSensor = useStore((s) => s.selectSensor);
  const accentColor = TYPE_COLORS[sensor.type?.toLowerCase() ?? 'generic'] ?? TYPE_COLORS.generic;

  const handleSelect = (e: React.MouseEvent) => {
    e.stopPropagation();
    console.log('[Dashboard] handleSelect click', { isSelected, sensorId: sensor.id, target: (e.target as HTMLElement).className });
    selectSensor(isSelected ? null : sensor.id);
  };

  return (
    <div
      className={`sensor-card${isSelected ? ' active' : ''}`}
      onClick={handleSelect}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => { if (e.key === 'Enter') handleSelect(e as unknown as React.MouseEvent); }}
    >
      <div className="name">{sensor.name}</div>
      {sensor.unit && <div className="unit">{sensor.unit}</div>}
      {latestMetric && (
        <div className="vals">
          <span>avg <strong>{latestMetric.avgValue.toFixed(2)}</strong></span>
          <span>min <strong>{latestMetric.minValue.toFixed(2)}</strong></span>
          <span>max <strong>{latestMetric.maxValue.toFixed(2)}</strong></span>
        </div>
      )}
      <div className="mini-chart">
        <MiniChart sensorId={sensor.id} height={52} accentColor={accentColor} />
      </div>
    </div>
  );
}