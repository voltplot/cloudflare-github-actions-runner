-- Lets the scheduled retention delete find expired samples without a table scan.
CREATE INDEX IF NOT EXISTS resource_trace_samples_received
  ON resource_trace_samples (received_at);
