import { applyD1Migrations, createScheduledController, env } from "cloudflare:test";
import { expect, inject, it } from "vite-plus/test";

import type { WorkerEnvironment } from "../../src/environment";
import worker from "../../src/index";

const day = 24 * 60 * 60 * 1_000;

it("the hourly cron deletes trace rows older than seven days and keeps newer ones", async () => {
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

  // SAFETY: the scheduled handler reads only the RESOURCE_METRICS binding.
  await worker.scheduled(createScheduledController(), env as WorkerEnvironment);

  const remaining = async (table: string) =>
    (await database.prepare(`SELECT runner_name FROM ${table}`).all<{ runner_name: string }>()).results;
  await expect(remaining("resource_trace_samples")).resolves.toEqual([{ runner_name: "cf-standard-3-job-2" }]);
  await expect(remaining("resource_trace_assignments")).resolves.toEqual([{ runner_name: "cf-standard-3-job-2" }]);
});
