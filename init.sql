-- Enable UUID generation (required before any table using gen_random_uuid)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- All timestamp columns use TIMESTAMPTZ (timestamp with time zone).
-- EF Core Npgsql provider maps C# DateTimeOffset → timestamptz automatically.
-- Plain TIMESTAMP silently discards timezone offset on write — use timestamptz for all application timestamps.

-- Sensors registry
CREATE TABLE IF NOT EXISTS sensors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    type VARCHAR(100) NOT NULL,
    unit VARCHAR(50),
    location VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw event log (for replay, auditing — not queried in hot path)
CREATE TABLE IF NOT EXISTS sensor_events (
    id BIGSERIAL PRIMARY KEY,
    sensor_id UUID REFERENCES sensors(id),
    value DOUBLE PRECISION NOT NULL,
    quality VARCHAR(20) DEFAULT 'good',
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_sensor_time ON sensor_events(sensor_id, received_at DESC);
CREATE INDEX idx_events_received ON sensor_events(received_at DESC);

-- Pre-aggregated metrics (queried by dashboard — low cardinality)
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id BIGSERIAL PRIMARY KEY,
    sensor_id UUID REFERENCES sensors(id),
    window_start TIMESTAMPTZ NOT NULL,
    window_duration_ms INT NOT NULL DEFAULT 1000,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    count INT NOT NULL DEFAULT 0,
    std_dev DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_metrics_sensor_window ON aggregated_metrics(sensor_id, window_start DESC);
CREATE INDEX idx_metrics_window ON aggregated_metrics(window_start DESC);

-- Alert definitions (threshold rules per sensor)
CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sensor_id UUID REFERENCES sensors(id),
    metric VARCHAR(100) NOT NULL, -- 'value', 'avg', 'std_dev'
    operator VARCHAR(20) NOT NULL, -- 'gt', 'lt', 'gte', 'lte', 'eq'
    threshold_value DOUBLE PRECISION NOT NULL,
    severity VARCHAR(20) DEFAULT 'warning', -- 'info', 'warning', 'critical'
    is_enabled BOOLEAN DEFAULT true,
    cooldown_seconds INT DEFAULT 60,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raised alerts
CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id UUID REFERENCES alert_rules(id),
    sensor_id UUID REFERENCES sensors(id),
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    value_at_trigger DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    status VARCHAR(20) DEFAULT 'active', -- 'active', 'acknowledged', 'resolved'
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ
);

CREATE INDEX idx_alerts_status ON alerts(status, triggered_at DESC);
CREATE INDEX idx_alerts_sensor ON alerts(sensor_id, triggered_at DESC);

-- SignalR scaling: presence tracking
CREATE TABLE IF NOT EXISTS hub_connections (
    connection_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    sensor_ids TEXT[], -- which sensors this connection subscribes to
    connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    disconnected_at TIMESTAMPTZ
);