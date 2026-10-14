import { createExecutionContext, env, runInDurableObject, waitOnExecutionContext } from "cloudflare:test";
import { expect, it } from "vite-plus/test";

import type { WorkerEnvironment } from "../../src/environment";
import worker from "../../src/index";
import { RUNNER_PROFILES } from "../../src/runner-profiles";

const account = "trace-write-fails";
const webhookSecret = "public-test-fixture";

async function signedWorkflowJobRequest(body: string): Promise<Request> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(webhookSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body)));
  const hex = Array.from(signature, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return new Request("https://runner.example.workers.dev/webhooks/github", {
    method: "POST",
    headers: { "X-GitHub-Event": "workflow_job", "X-Hub-Signature-256": `sha256=${hex}` },
    body,
  });
}

it("keeps runner ownership when the trace assignment write fails", async () => {
  const scheduler = env.RUNNER_SCHEDULER.getByName(account);
  const [displacedJobId, actualJobId] = ["700", "701"];
  const runnerName = `cf-standard-3-job-${displacedJobId}`;
  const submit = (jobId: string) =>
    scheduler.submit({
      jobId,
      runnerName: `cf-standard-3-job-${jobId}`,
      headSha: "0123456789abcdef0123456789abcdef01234567",
      target: { owner: "biw", repository: "runner-poc" },
      installationId: 42,
      profile: RUNNER_PROFILES["standard-3"],
      workerOrigin: "https://runner.example.workers.dev",
    });
  await submit(displacedJobId);
  await submit(actualJobId);
  await runInDurableObject(scheduler, async (_instance, state) => {
    // Apply admission capacity without a real Containers API request.
    state.storage.sql.exec(
      `UPDATE scheduler_slots
       SET configuration_state = 'ready', configuration_owner_job_id = NULL, applied_max_instances = 2,
           desired_max_instances = 2, reserved_count = 2, capacity_debounce_until = 0,
           capacity_update_in_progress = 0, capacity_reclaim_pending = 0
       WHERE slot_id = 'preset:standard-3'`,
    );
    state.storage.sql.exec(
      "UPDATE scheduler_jobs SET status = 'provisioning', slot_id = 'preset:standard-3' WHERE job_id = ?",
      displacedJobId,
    );
    await state.storage.deleteAlarm();
  });
  await scheduler.runnerProvisioned(displacedJobId, runnerName, 7_001);
  await scheduler.runnerStarted(runnerName);

  // GitHub gives the displaced job's runner another job while D1 is full.
  const full = (): never => {
    throw new Error("D1_ERROR: Exceeded maximum DB size");
  };
  const fullD1: D1Database = { prepare: full, batch: full, exec: full, withSession: full, dump: full };
  const ctx = createExecutionContext();
  const response = await worker.fetch(
    await signedWorkflowJobRequest(
      JSON.stringify({
        action: "in_progress",
        installation: { id: 42 },
        repository: { full_name: "biw/runner-poc" },
        workflow_job: {
          id: Number(actualJobId),
          labels: ["cloudflare-standard-3"],
          runner_name: runnerName,
          runner_id: 7_001,
        },
      }),
    ),
    // SAFETY: a job-started webhook reads only the bindings set here and the test pool's
    // scheduler and provisioning workflow; no GitHub App secret is reached.
    {
      ...(env as WorkerEnvironment),
      CLOUDFLARE_ACCOUNT_ID: account,
      GITHUB_RUNNER_OWNER: "biw",
      GITHUB_APP_WEBHOOK_SECRET: webhookSecret,
      RESOURCE_METRICS: fullD1,
    },
    ctx,
  );
  await waitOnExecutionContext(ctx);

  expect(response.status).toBe(500);
  await runInDurableObject(scheduler, (_instance, state) => {
    expect(
      state.storage.sql
        .exec(
          "SELECT source_job_id, assigned_job_id, reserved FROM scheduler_jit_runners WHERE runner_name = ?",
          runnerName,
        )
        .toArray(),
    ).toEqual([{ source_job_id: displacedJobId, assigned_job_id: actualJobId, reserved: 1 }]);
    expect(
      state.storage.sql.exec("SELECT runner_name FROM scheduler_jobs WHERE job_id = ?", displacedJobId).toArray(),
    ).toEqual([{ runner_name: runnerName }]);
  });
});
