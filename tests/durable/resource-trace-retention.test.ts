import { applyD1Migrations, createExecutionContext, env, waitOnExecutionContext } from "cloudflare:test";
import { expect, inject, it } from "vite-plus/test";

import type { WorkerEnvironment } from "../../src/environment";
import worker from "../../src/index";
import { signedWorkflowJobRequest, webhookSecret } from "./signed-webhook";

const day = 24 * 60 * 60 * 1_000;

it("a completed job deletes trace rows older than seven days and keeps newer ones", async () => {
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

  const ctx = createExecutionContext();
  const response = await worker.fetch(
    await signedWorkflowJobRequest(
      JSON.stringify({
        action: "completed",
        installation: { id: 42 },
        repository: { full_name: "biw/runner-poc" },
        workflow_job: { id: 900, labels: ["cloudflare-standard-3"], runner_name: "cf-standard-3-job-900" },
      }),
    ),
    // SAFETY: a job-completed webhook reads only the bindings set here and the test pool's
    // scheduler, provisioning workflow and metrics database; no GitHub App secret is reached.
    {
      ...(env as WorkerEnvironment),
      CLOUDFLARE_ACCOUNT_ID: "trace-retention",
      GITHUB_RUNNER_OWNER: "biw",
      GITHUB_APP_WEBHOOK_SECRET: webhookSecret,
    },
    ctx,
  );
  await waitOnExecutionContext(ctx);

  expect(response.status).toBe(202);
  const remaining = async (table: string) =>
    (await database.prepare(`SELECT runner_name FROM ${table}`).all<{ runner_name: string }>()).results;
  await expect(remaining("resource_trace_samples")).resolves.toEqual([{ runner_name: "cf-standard-3-job-2" }]);
  await expect(remaining("resource_trace_assignments")).resolves.toEqual([{ runner_name: "cf-standard-3-job-2" }]);
});
