import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vite-plus/test";

import type { SchedulerJobInput } from "../../src/account-runner-scheduler";
import { RUNNER_PROFILES } from "../../src/runner-profiles";

const target = { owner: "biw", repository: "runner-poc" };
const profile = RUNNER_PROFILES["standard-3"];

function job(jobId: string, runnerName: string, cacheScope: string): SchedulerJobInput {
  return {
    jobId,
    headSha: "0123456789abcdef0123456789abcdef01234567",
    runnerName,
    target,
    installationId: 42,
    profile,
    workerOrigin: "https://runner.example.workers.dev",
    cacheScope: { scope: cacheScope, fallbackScope: "refs/heads/main", writeAllowed: true },
  };
}

async function provisionRunner(
  scheduler: DurableObjectStub<import("../../src/account-runner-scheduler").AccountRunnerScheduler>,
  jobId: string,
  runnerName: string,
  runnerId: number,
): Promise<void> {
  await runInDurableObject(scheduler, async (_instance, state) => {
    // Admission capacity is separately covered by scheduler-policy tests. This
    // test prepares two live, compatible JIT runners to reproduce GitHub's
    // out-of-order assignment race without calling the Containers API. Mark
    // the pending capacity operation as already applied so the Durable Object
    // alarm cannot race this test and issue a real capacity request.
    state.storage.sql.exec(
      `UPDATE scheduler_slots
       SET applied_max_instances = desired_max_instances,
           capacity_debounce_until = 0,
           capacity_update_in_progress = 0
       WHERE slot_id = 'preset:standard-3'`,
    );
    state.storage.sql.exec(
      "UPDATE scheduler_jobs SET status = 'provisioning', slot_id = 'preset:standard-3' WHERE job_id = ?",
      jobId,
    );
  });
  await scheduler.runnerProvisioned(jobId, runnerName, runnerId);
  await scheduler.runnerStarted(runnerName);
}

describe("AccountRunnerScheduler JIT cache assignments", () => {
  it("carries the queued head SHA into the provisioning plan", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("head-sha-provisioning");
    const queuedJob = job("50", "cf-standard-3-job-50", "refs/heads/main");

    await scheduler.submit(queuedJob);
    await runInDurableObject(scheduler, async (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE scheduler_jobs SET status = 'provisioning', slot_id = 'preset:standard-3' WHERE job_id = ?`,
        queuedJob.jobId,
      );
      state.storage.sql.exec(
        `UPDATE scheduler_slots
         SET applied_max_instances = desired_max_instances,
             capacity_update_in_progress = 0,
             capacity_reclaim_pending = 0
         WHERE slot_id = 'preset:standard-3'`,
      );
    });

    await expect(scheduler.claimProvisioning(queuedJob.jobId)).resolves.toMatchObject({
      kind: "provision",
      jobId: queuedJob.jobId,
      headSha: queuedJob.headSha,
    });
  });

  it("keeps cache access with each runner when two compatible JIT runners cross-assign jobs", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("cross-assignment");
    const firstJob = job("100", "cf-standard-3-job-100", "refs/pull/100/merge");
    const secondJob = job("200", "cf-standard-3-job-200", "refs/pull/200/merge");

    await scheduler.submit(firstJob);
    await scheduler.submit(secondJob);
    await provisionRunner(scheduler, firstJob.jobId, firstJob.runnerName, 1_001);
    await provisionRunner(scheduler, secondJob.jobId, secondJob.runnerName, 2_001);

    // GitHub gives runner 100 job 200 first. The scheduler requeues job 100
    // under a new JIT name, so runner 200 no longer has a job row named after
    // it when GitHub later assigns runner 200 to job 100.
    await scheduler.workflowJobStarted({
      jobId: secondJob.jobId,
      runnerName: firstJob.runnerName,
      runnerId: 1_001,
      target,
      profile,
    });
    await scheduler.workflowJobStarted({
      jobId: firstJob.jobId,
      runnerName: secondJob.runnerName,
      runnerId: 2_001,
      target,
      profile,
    });

    await expect(scheduler.cacheAssignment(firstJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: secondJob.jobId,
      cacheScope: { scope: secondJob.cacheScope?.scope, fallbackScope: "refs/heads/main", writeAllowed: true },
    });
    await expect(scheduler.cacheAssignment(secondJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: firstJob.jobId,
      cacheScope: { scope: firstJob.cacheScope?.scope, fallbackScope: "refs/heads/main", writeAllowed: true },
    });
  });

  it("does not grant cache access before GitHub confirms the runner assignment", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("unassigned-runner");
    const queuedJob = job("300", "cf-standard-3-job-300", "refs/pull/300/merge");

    await scheduler.submit(queuedJob);
    await provisionRunner(scheduler, queuedJob.jobId, queuedJob.runnerName, 3_001);

    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
  });

  it("does not record an assignment across repository or machine-profile boundaries", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("isolated-assignment");
    const queuedJob = job("400", "cf-standard-3-job-400", "refs/pull/400/merge");

    await scheduler.submit(queuedJob);
    await provisionRunner(scheduler, queuedJob.jobId, queuedJob.runnerName, 4_001);
    await scheduler.workflowJobStarted({
      jobId: queuedJob.jobId,
      runnerName: queuedJob.runnerName,
      runnerId: 4_001,
      target: { owner: "biw", repository: "different-repository" },
      profile,
    });

    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
  });
});

describe("AccountRunnerScheduler stopped runners", () => {
  // The Worker's legacy repository, whose token the test environment provides.
  const legacyTarget = { owner: "biw", repository: "cloudflare-github-actions-runner" };
  const legacyJob = (jobId: string): SchedulerJobInput => ({
    ...job(jobId, `cf-standard-3-job-${jobId}`, "refs/heads/main"),
    target: legacyTarget,
    installationId: undefined,
  });

  it("gives a fresh runner to a job GitHub still has queued after its runner exits idle", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("idle-exit-requeue");
    const heldJob = legacyJob("900");

    await scheduler.submit(heldJob);
    await provisionRunner(scheduler, heldJob.jobId, heldJob.runnerName, 9_001);
    await runInDurableObject(scheduler, async (instance, state) => {
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockImplementation(async (_input, init) =>
          init?.method === "DELETE" ? new Response(null, { status: 204 }) : Response.json({ status: "queued" }),
        );
      try {
        await instance.runnerStopped(heldJob.runnerName, { exitCode: 0, reason: "exit" });
        state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0 WHERE job_id = ?", heldJob.jobId);
        await instance.alarm();
      } finally {
        fetch.mockRestore();
      }
      const [row] = state.storage.sql
        .exec<{ status: string; runner_name: string }>(
          "SELECT status, runner_name FROM scheduler_jobs WHERE job_id = ?",
          heldJob.jobId,
        )
        .toArray();
      expect(row).toMatchObject({ runner_name: `${heldJob.runnerName}-r2` });
      expect(["queued", "admitted"]).toContain(row?.status);
    });
  });

  it("deregisters only the runner whose Container stopped", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("stopped-runner-only");
    const sourceJob = legacyJob("1000");
    const otherJob = legacyJob("1001");

    await scheduler.submit(sourceJob);
    await scheduler.submit(otherJob);
    await provisionRunner(scheduler, sourceJob.jobId, sourceJob.runnerName, 10_001);
    await provisionRunner(scheduler, otherJob.jobId, otherJob.runnerName, 10_002);

    const deletes: string[] = [];
    await runInDurableObject(scheduler, async (instance, state) => {
      const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
        if (init?.method !== "DELETE") {
          return Response.json({ status: "completed" });
        }
        deletes.push(String(input).split("/").at(-1) ?? "");
        if (deletes.length === 1) {
          // GitHub gives the source job to the other, live runner while the
          // stopped runner's deregistration is in flight, and that fails.
          await instance.workflowJobStarted({
            jobId: sourceJob.jobId,
            runnerName: otherJob.runnerName,
            runnerId: 10_002,
            target: legacyTarget,
            profile,
          });
          return new Response(null, { status: 500 });
        }
        return new Response(null, { status: 204 });
      });
      try {
        await instance.runnerStopped(sourceJob.runnerName, { exitCode: 0, reason: "exit" });
        expect(
          state.storage.sql
            .exec("SELECT status, runner_name FROM scheduler_jobs WHERE job_id = ?", sourceJob.jobId)
            .toArray(),
        ).toEqual([{ status: "running", runner_name: otherJob.runnerName }]);
        await instance.workflowJobCompleted(sourceJob.jobId);
        // .5's recovery left cleanup retries on runners that never stopped.
        state.storage.sql.exec(
          "UPDATE scheduler_jobs SET runner_cleanup_state = 'retry' WHERE job_id = ?",
          sourceJob.jobId,
        );
        state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0, runner_cleanup_due_at = 0");
        await instance.alarm();
      } finally {
        fetch.mockRestore();
      }
    });
    expect(deletes).toEqual(["10001"]);
  });
});
