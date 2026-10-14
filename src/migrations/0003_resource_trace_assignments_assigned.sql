-- Lets the retention delete find expired assignments without a table scan.
CREATE INDEX IF NOT EXISTS resource_trace_assignments_assigned
  ON resource_trace_assignments (assigned_at);
