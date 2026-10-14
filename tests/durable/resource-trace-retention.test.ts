import { applyD1Migrations, createExecutionContext, env, waitOnExecutionContext } from "cloudflare:test";
import { expect, inject, it } from "vite-plus/test";

import type { WorkerEnvironment } from "../../src/environment";
import worker from "../../src/index";
import { createResourceTraceAuthorization } from "../../src/resource-traces";

const day = 24 * 60 * 60 * 1_000;
const signingKey = "public-test-fixture";

it("a trace upload deletes trace rows older than seven days and keeps newer ones", async () => {
  const database = env.RESOURCE_METRICS;
  await applyD1Migrations(database, inject("resourceMetricsMigrations"));
  const now = Date.now();
  const rows = [
    ["cf-standard-3-job-1", now - 8 * day],
    ["cf-standard-3-job-2", now - 6 * day],
  ] as const;
  await database.batch(
    rows.flatMap(([runnerName, at]) => [
      database
        .prepare(
          `INSERT INTO resource_trace_samples VALUES (?1, '1', 'biw/example', 0, '2026-01-01T00:00:00Z', 1,
           'Run tests', 0, 0, 0, 0, 0, 0, 0, ?2)`,
        )
        .bind(runnerName, at),
      database
        .prepare("INSERT INTO resource_trace_assignments VALUES (?1, '1', 'biw/example', ?2)")
        .bind(runnerName, at),
    ]),
  );
  const authorization = await createResourceTraceAuthorization(signingKey, {
    runnerName: "cf-standard-3-job-3",
    jobId: "3",
    repository: "biw/example",
  });
  const sample = {
    timestamp: "2026-01-01T00:00:00Z",
    elapsedSeconds: 0,
    intervalSeconds: 1,
    phase: "Run tests",
    cpuTotalUsec: 0,
    cpuDeltaUsec: 0,
    cpuCoresAvg: 0,
    memoryCurrentBytes: 0,
    memoryPeakBytes: 0,
    rootDiskUsedBytes: 0,
    rootDiskDeltaBytes: 0,
  };

  const ctx = createExecutionContext();
  const response = await worker.fetch(
    new Request("https://runner.example.workers.dev/v1/resource-traces", {
      method: "POST",
      headers: { Authorization: `Bearer ${authorization}` },
      body: JSON.stringify({ samples: [sample] }),
    }),
    // SAFETY: a trace upload reads only the signing key set here and the metrics database.
    { ...(env as WorkerEnvironment), RESOURCE_TRACE_SIGNING_KEY: signingKey },
    ctx,
  );
  await waitOnExecutionContext(ctx);

  expect(response.status).toBe(202);
  const remaining = async (table: string) =>
    (
      await database.prepare(`SELECT runner_name FROM ${table} ORDER BY runner_name`).all<{ runner_name: string }>()
    ).results.map((row) => row.runner_name);
  await expect(remaining("resource_trace_samples")).resolves.toEqual(["cf-standard-3-job-2", "cf-standard-3-job-3"]);
  await expect(remaining("resource_trace_assignments")).resolves.toEqual(["cf-standard-3-job-2"]);
});
