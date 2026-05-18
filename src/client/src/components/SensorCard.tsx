import { useStore } from '../lib/store';
import type { Sensor } from '../types';

interface Props {
  sensor: Sensor;
  isSelected: boolean;
  latestMetric?: { avgValue: number; minValue: number; maxValue: number };
}

export function SensorCard({ sensor, isSelected, latestMetric }: Props) {
  const selectSensor = useStore((s) => s.selectSensor);
  const isAllSensors = sensor.id === '__all__';

  const handleSelect = () => {
    if (isAllSensors) {
      selectSensor(null);
      return;
    }

    selectSensor(isSelected ? null : sensor.id);
  };

  return (
    <div
      className={`sensor-card${isSelected ? ' active' : ''}`}
      onClick={handleSelect}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => e.key === 'Enter' && handleSelect()}
    >
      <div className="name">{sensor.name}</div>
      <div className="unit">{sensor.unit}</div>
      {latestMetric && (
        <div className="vals">
          <span>avg <strong>{latestMetric.avgValue.toFixed(2)}</strong></span>
          <span>min <strong>{latestMetric.minValue.toFixed(2)}</strong></span>
          <span>max <strong>{latestMetric.maxValue.toFixed(2)}</strong></span>
        </div>
      )}
    </div>
  );
}