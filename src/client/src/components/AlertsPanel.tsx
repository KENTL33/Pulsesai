import { useStore } from '../lib/store';

export function AlertsPanel() {
  const alerts = useStore((s) => s.alerts);

  return (
    <div className="alerts-panel">
      <h3>Alerts ({alerts.length})</h3>
      {alerts.length === 0 ? (
        <div className="empty-state">No active alerts</div>
      ) : (
        alerts.map((alert) => (
          <div key={alert.id} className={`alert-item ${alert.severity}`}>
            <div className="msg">{alert.message}</div>
            <div className="meta">
              {alert.severity} · {new Date(alert.triggeredAt).toLocaleTimeString()}
            </div>
          </div>
        ))
      )}
    </div>
  );
}