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

describe("AccountRunnerScheduler idle runners", () => {
  it("asks GitHub about a runner whose job completed or moved to another runner during provisioning", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("completed-during-provisioning");
    const queuedJob = job("500", "cf-standard-3-job-500", "refs/heads/main");

    await scheduler.submit(queuedJob);
    await runInDurableObject(scheduler, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET status = 'releasing', runner_name = 'cf-standard-3-job-499' WHERE job_id = ?",
        queuedJob.jobId,
      );
    });
    await scheduler.runnerProvisioned(queuedJob.jobId, queuedJob.runnerName, 5_001);

    // The test Worker has no GitHub App key, so reaching the token request proves the runner was found.
    await runInDurableObject(scheduler, async (instance) => {
      await expect(instance.deregisterIdleRunner(queuedJob.runnerName)).rejects.toThrow(
        "No GitHub App installation token is available for biw",
      );
    });
  });

  it("keeps a runner whose GitHub assignment was delivered without asking GitHub", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("assigned-runner");
    const queuedJob = job("600", "cf-standard-3-job-600", "refs/heads/main");

    await scheduler.submit(queuedJob);
    await provisionRunner(scheduler, queuedJob.jobId, queuedJob.runnerName, 6_001);
    await scheduler.workflowJobStarted({
      jobId: queuedJob.jobId,
      runnerName: queuedJob.runnerName,
      runnerId: 6_001,
      target,
      profile,
    });

    // Asking GitHub would fail here: the test Worker has no GitHub App key.
    await expect(scheduler.deregisterIdleRunner(queuedJob.runnerName)).resolves.toBe(false);
  });

  it("keeps a runner whose assignment arrived before its job was queued, then hands it that job", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("started-before-queued");
    const queuedJob = job("700", "cf-standard-3-job-700", "refs/heads/main");
    const earlyJob = job("701", "cf-standard-3-job-701", "refs/heads/main");

    await scheduler.submit(queuedJob);
    await provisionRunner(scheduler, queuedJob.jobId, queuedJob.runnerName, 7_001);
    await scheduler.workflowJobStarted({
      jobId: earlyJob.jobId,
      runnerName: queuedJob.runnerName,
      runnerId: 7_001,
      target,
      profile,
    });

    await expect(scheduler.deregisterIdleRunner(queuedJob.runnerName)).resolves.toBe(false);

    await scheduler.submit(earlyJob);
    await runInDurableObject(scheduler, async (_instance, state) => {
      const running = state.storage.sql
        .exec("SELECT job_id, runner_name FROM scheduler_jobs WHERE status = 'running'")
        .toArray();
      expect(running).toEqual([{ job_id: earlyJob.jobId, runner_name: queuedJob.runnerName }]);
    });
  });

  it("does not deregister a runner running another job when its source job's completion is recovered", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("recovery-of-reassigned-runner");
    const sourceJob = {
      ...job("750", "cf-standard-3-job-750", "refs/heads/main"),
      target: { owner: "biw", repository: "cloudflare-github-actions-runner" },
      installationId: undefined,
    };

    await scheduler.submit(sourceJob);
    await provisionRunner(scheduler, sourceJob.jobId, sourceJob.runnerName, 7_501);
    await scheduler.workflowJobStarted({
      jobId: "751",
      runnerName: sourceJob.runnerName,
      runnerId: 7_501,
      target: sourceJob.target,
      profile,
    });
    await scheduler.workflowJobCompleted(sourceJob.jobId);

    const requests: string[] = [];
    await runInDurableObject(scheduler, async (instance, state) => {
      state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0 WHERE job_id = ?", sourceJob.jobId);
      const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
        requests.push(`${init?.method ?? "GET"} ${String(input)}`);
        return new Response(null, { status: 204 });
      });
      try {
        await instance.alarm();
      } finally {
        fetch.mockRestore();
      }
    });
    expect(requests.filter((request) => request.startsWith("DELETE"))).toEqual([]);
  });

  it.each(["GET", "DELETE"])("keeps a runner whose assignment lands during the GitHub %s", async (landsDuring) => {
    const scheduler = env.RUNNER_SCHEDULER.getByName(`assigned-during-${landsDuring}`);
    // The Worker's legacy repository, whose token the test environment provides.
    const legacyTarget = { owner: "biw", repository: "cloudflare-github-actions-runner" };
    const sourceJob = {
      ...job("800", "cf-standard-3-job-800", "refs/heads/main"),
      target: legacyTarget,
      installationId: undefined,
    };
    const assignedJob = { ...sourceJob, jobId: "801", runnerName: "cf-standard-3-job-801" };

    await scheduler.submit(sourceJob);
    await scheduler.submit(assignedJob);
    await provisionRunner(scheduler, sourceJob.jobId, sourceJob.runnerName, 8_001);
    await runInDurableObject(scheduler, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET status = 'running', runner_name = 'cf-standard-3-job-799' WHERE job_id = ?",
        sourceJob.jobId,
      );
    });

    const requests: string[] = [];
    await runInDurableObject(scheduler, async (instance) => {
      const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
        const method = init?.method ?? "GET";
        requests.push(method);
        if (method === landsDuring) {
          await instance.workflowJobStarted({
            jobId: assignedJob.jobId,
            runnerName: sourceJob.runnerName,
            runnerId: 8_001,
            target: legacyTarget,
            profile,
          });
        }
        return method === "DELETE"
          ? new Response(null, { status: 204 })
          : Response.json({ runners: [{ id: 8_001, name: sourceJob.runnerName, busy: false }] });
      });
      try {
        await expect(instance.deregisterIdleRunner(sourceJob.runnerName)).resolves.toBe(false);
      } finally {
        fetch.mockRestore();
      }
    });
    expect(requests).toEqual(landsDuring === "GET" ? ["GET"] : ["GET", "DELETE"]);
  });
});
