import { useSignalR } from './lib/signalr';
import { Dashboard } from './pages/Dashboard';
import { ConnectionStatus } from './components/ConnectionStatus';

function Header() {
  return (
    <header className="header">
      <h1>Pulses</h1>
      <ConnectionStatus />
    </header>
  );
}

export function App() {
  useSignalR();
  return (
    <>
      <Header />
      <Dashboard />
    </>
  );
}