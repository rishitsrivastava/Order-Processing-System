-- idempotency / dedupe table
CREATE TABLE IF NOT EXISTS processed_events (
  id SERIAL PRIMARY KEY,
  event_id VARCHAR(255) UNIQUE NOT NULL,
  order_id VARCHAR(100),
  consumer VARCHAR(100),
  processed_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_orderid ON processed_events(order_id);
