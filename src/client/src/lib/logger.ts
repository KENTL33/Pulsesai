const CORRELATION_KEY = 'pulses_cid';
const BATCH_INTERVAL_MS = 30_000;
const BATCH_URL = '/api/logs/ingest';

interface LogEntry {
  level: 'info' | 'warn' | 'error';
  message: string;
  timestamp: string;
  correlationId?: string;
  [key: string]: unknown;
}

let queue: LogEntry[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;

function getCorrelationId(): string {
  const stored = sessionStorage.getItem(CORRELATION_KEY);
  if (stored) return stored;
  const cid = crypto.randomUUID();
  sessionStorage.setItem(CORRELATION_KEY, cid);
  return cid;
}

function enqueue(entry: LogEntry): void {
  queue.push({ ...entry, correlationId: getCorrelationId() });
  if (!flushTimer) flushTimer = setTimeout(flush, BATCH_INTERVAL_MS);
}

function flush(): void {
  flushTimer = null;
  if (queue.length === 0) return;
  const batch = queue.splice(0);
  fetch(BATCH_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(batch),
  }).catch(() => {}); // fire-and-forget
}

export const logger = {
  info(message: string, meta?: Record<string, unknown>) {
    enqueue({ level: 'info', message, timestamp: new Date().toISOString(), ...meta });
  },
  warn(message: string, meta?: Record<string, unknown>) {
    enqueue({ level: 'warn', message, timestamp: new Date().toISOString(), ...meta });
  },
  error(message: string, meta?: Record<string, unknown>) {
    enqueue({ level: 'error', message, timestamp: new Date().toISOString(), ...meta });
  },
};

// Capture browser errors globally
window.addEventListener('error', (evt) => {
  logger.error(`[UNCAUGHT] ${evt.message}`, {
    filename: evt.filename,
    lineno: evt.lineno,
    colno: evt.colno,
  });
});

window.addEventListener('unhandledrejection', (evt) => {
  logger.error(`[UNHANDLED REJECTION] ${evt.reason}`, {});
});