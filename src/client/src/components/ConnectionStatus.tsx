import { useStore } from '../lib/store';

export function ConnectionStatus() {
  const state = useStore((s) => s.connectionState);
  return (
    <div className="conn-badge">
      <div className={`conn-dot ${state}`} />
      {state === 'connected' ? 'Live' : state === 'connecting' ? 'Connecting' : 'Disconnected'}
    </div>
  );
}