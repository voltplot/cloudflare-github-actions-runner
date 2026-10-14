import { env, evictDurableObject, runInDurableObject } from "cloudflare:test";
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

async function setRuntime(runnerName: string, state: "running" | "starting" | "stopped" | "error"): Promise<void> {
  const response = await env.RUNNER_STANDARD_3.getByName(runnerName).fetch(`https://runtime/${state}`, {
    method: "POST",
  });
  expect(response.status).toBe(204);
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
  await setRuntime(runnerName, "running");
  await runInDurableObject(scheduler, (_instance, state) => {
    state.storage.sql.exec(
      "UPDATE scheduler_jit_runners SET provisioning_deadline = 0 WHERE runner_name = ?",
      runnerName,
    );
  });
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
        ).toEqual([{ status: "running", runner_name: sourceJob.runnerName }]);
        await instance.workflowJobCompleted(sourceJob.jobId);
        // Legacy cleanup retries must never delete a live runner.
        state.storage.sql.exec(
          "UPDATE scheduler_jit_runners SET runner_cleanup_state = 'retry', runner_cleanup_due_at = 0 WHERE runner_name = ?",
          otherJob.runnerName,
        );
        state.storage.sql.exec("UPDATE scheduler_jit_runners SET runner_cleanup_due_at = 0");
        await instance.alarm();
      } finally {
        fetch.mockRestore();
      }
    });
    expect(deletes).toEqual(["10001", "10001"]);
  });

  it("keeps every crossed runner reserved through completion, alarms and restart within the completion grace", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("runner-reservations");
    const first = legacyJob("1100");
    const second = legacyJob("1101");
    const third = legacyJob("1102");
    await scheduler.submit(first);
    await scheduler.submit(second);
    await provisionRunner(scheduler, first.jobId, first.runnerName, 11_001);
    await provisionRunner(scheduler, second.jobId, second.runnerName, 11_002);
    await scheduler.workflowJobStarted({
      jobId: first.jobId,
      runnerName: second.runnerName,
      runnerId: 11_002,
      target: legacyTarget,
      profile,
    });
    await scheduler.workflowJobCompleted(first.jobId);
    await scheduler.submit(third);
    await scheduler.workflowJobStarted({
      jobId: third.jobId,
      runnerName: first.runnerName,
      runnerId: 11_001,
      target: legacyTarget,
      profile,
    });
    await scheduler.workflowJobCompleted(second.jobId);
    await scheduler.workflowJobCompleted(third.jobId);
    const reserved = async () => {
      const status = await scheduler.status();
      return status.slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount;
    };
    expect(await reserved()).toBe(2);
    await runInDurableObject(scheduler, async (instance, state) => {
      state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0");
      await instance.alarm();
      expect(
        state.storage.sql
          .exec("SELECT runner_name, reserved FROM scheduler_jit_runners WHERE reserved = 1 ORDER BY runner_name")
          .toArray(),
      ).toEqual([
        { runner_name: first.runnerName, reserved: 1 },
        { runner_name: second.runnerName, reserved: 1 },
      ]);
      await state.storage.deleteAlarm();
    });
    await evictDurableObject(scheduler);
    expect(await reserved()).toBe(2);
    await runInDurableObject(scheduler, async (instance) => {
      const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 204 }));
      try {
        await instance.runnerStopped(second.runnerName, { exitCode: 0, reason: "exit" });
        expect((await instance.status()).slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(
          1,
        );
        await instance.runnerStopped(second.runnerName, { exitCode: 0, reason: "exit" });
        expect((await instance.status()).slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(
          1,
        );
        await instance.runnerStopped(first.runnerName, { exitCode: 0, reason: "exit" });
      } finally {
        fetch.mockRestore();
      }
    });
    expect(await reserved()).toBe(0);
  });

  it("retains a reservation when completion and provisioning failure race a recorded JIT start", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("late-jit-start");
    const source = legacyJob("1200");
    await scheduler.submit(source);
    await runInDurableObject(scheduler, (_instance, state) => {
      state.storage.sql.exec("UPDATE scheduler_jobs SET status = 'provisioning' WHERE job_id = ?", source.jobId);
    });
    await scheduler.workflowJobCompleted(source.jobId);
    await scheduler.runnerProvisioned(source.jobId, source.runnerName, 12_001);
    await scheduler.provisioningFailed(source.jobId, source.runnerName, "late workflow failure");
    await setRuntime(source.runnerName, "running");
    const status = await scheduler.status();
    expect(status.slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(1);
  });

  it("migrates legacy orphan and releasing runners without losing reservations on restart", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("legacy-reservations");
    const first = legacyJob("1300");
    const second = legacyJob("1301");
    const idle = legacyJob("1302");
    await scheduler.submit(first);
    await scheduler.submit(second);
    await provisionRunner(scheduler, first.jobId, first.runnerName, 13_001);
    await provisionRunner(scheduler, second.jobId, second.runnerName, 13_002);
    await scheduler.submit(idle);
    await provisionRunner(scheduler, idle.jobId, idle.runnerName, 13_003);
    await runInDurableObject(scheduler, async (_instance, state) => {
      state.storage.sql.exec(
        `CREATE TABLE legacy_jit (
          runner_name TEXT PRIMARY KEY, source_job_id TEXT NOT NULL, github_owner TEXT NOT NULL,
          github_repository TEXT NOT NULL, profile_key TEXT NOT NULL, assigned_job_id TEXT,
          assignment_observed INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        INSERT INTO legacy_jit SELECT runner_name, source_job_id, github_owner, github_repository, profile_key,
          assigned_job_id, assignment_observed, created_at, updated_at FROM scheduler_jit_runners;
        DROP TABLE scheduler_jit_runners;
        ALTER TABLE legacy_jit RENAME TO scheduler_jit_runners;`,
      );
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET runner_name = ?, runner_id = ?, status = 'releasing' WHERE job_id = ?",
        second.runnerName,
        13_002,
        first.jobId,
      );
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET runner_name = ?, runner_id = NULL, slot_id = NULL, status = 'queued' WHERE job_id = ?",
        second.runnerName + "-r2",
        second.jobId,
      );
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET runner_name = ?, runner_id = NULL, slot_id = NULL, status = 'queued' WHERE job_id = ?",
        idle.runnerName + "-r2",
        idle.jobId,
      );
      state.storage.sql.exec(
        "INSERT INTO scheduler_events (job_id, kind, detail_json, created_at) VALUES (?, 'github-job-requeued-after-idle-runner', ?, ?)",
        idle.jobId,
        JSON.stringify({ runnerName: idle.runnerName + "-r2", retryAttempt: 2 }),
        Date.now(),
      );
      const incomplete = [
        { runnerName: "legacy-missing-source", sourceJobId: "unknown-job" },
        { runnerName: "legacy-missing-identity", sourceJobId: idle.jobId },
        { runnerName: "legacy-missing-slot", sourceJobId: first.jobId },
      ];
      const legacyCreatedAt = Date.now() + 1;
      for (const runner of incomplete) {
        state.storage.sql.exec(
          `INSERT INTO scheduler_jit_runners
           (runner_name, source_job_id, github_owner, github_repository, profile_key, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          runner.runnerName,
          runner.sourceJobId,
          legacyTarget.owner,
          legacyTarget.repository,
          runner.runnerName === "legacy-missing-slot" ? "custom-unknown" : profile.key,
          legacyCreatedAt,
          legacyCreatedAt,
        );
      }
      state.storage.sql.exec(
        "INSERT INTO scheduler_events (job_id, kind, detail_json, created_at) VALUES (?, 'jit-runner-created', ?, ?)",
        first.jobId,
        JSON.stringify({ runnerId: 13_004 }),
        legacyCreatedAt,
      );
      state.storage.sql.exec(
        // Restart re-arms alarms; defer capacity work until migration assertions finish.
        "UPDATE scheduler_slots SET reserved_count = 1, desired_max_instances = applied_max_instances, capacity_debounce_until = ?",
        Date.now() + 60_000,
      );
      await state.storage.deleteAlarm();
    });
    const restart = async () => {
      await evictDurableObject(scheduler);
      const status = await scheduler.status();
      expect(status.slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(4);
      await runInDurableObject(scheduler, async (_instance, state) => {
        expect(
          state.storage.sql
            .exec("SELECT runner_name, runner_id, reserved FROM scheduler_jit_runners ORDER BY runner_name")
            .toArray(),
        ).toEqual([
          { runner_name: first.runnerName, runner_id: 13_001, reserved: 1 },
          { runner_name: second.runnerName, runner_id: 13_002, reserved: 1 },
          { runner_name: idle.runnerName, runner_id: 13_003, reserved: 1 },
          { runner_name: "legacy-missing-identity", runner_id: null, reserved: 1 },
          { runner_name: "legacy-missing-slot", runner_id: null, reserved: 0 },
          { runner_name: "legacy-missing-source", runner_id: null, reserved: 0 },
        ]);
        expect(status.events.filter((event) => event.kind === "legacy-runner-reservation-released")).toHaveLength(2);
        await state.storage.deleteAlarm();
      });
    };
    await restart();
    await restart();
    await setRuntime(idle.runnerName, "stopped");
    await setRuntime("legacy-missing-identity", "stopped");
    await runInDurableObject(scheduler, async (instance, state) => {
      state.storage.sql.exec("UPDATE scheduler_jit_runners SET provisioning_deadline = 0");
      const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 204 }));
      try {
        await instance.alarm();
        expect(
          state.storage.sql
            .exec(
              "SELECT reserved, container_stop_reason FROM scheduler_jit_runners WHERE runner_name IN (?, ?) ORDER BY runner_name",
              idle.runnerName,
              "legacy-missing-identity",
            )
            .toArray(),
        ).toEqual([
          { reserved: 0, container_stop_reason: "runtime-not-running" },
          { reserved: 0, container_stop_reason: "runtime-not-running" },
        ]);
      } finally {
        fetch.mockRestore();
      }
    });
  });

  it.each(["admitted", "provisioning"])(
    "cancels %s demand without failing a job already assigned elsewhere",
    async (pendingStatus) => {
      const scheduler = env.RUNNER_SCHEDULER.getByName("unstarted-demand-" + pendingStatus);
      const source = legacyJob("1400");
      const actual = legacyJob("1401");
      await scheduler.submit(source);
      await provisionRunner(scheduler, source.jobId, source.runnerName, 14_001);
      await scheduler.submit(actual);
      await runInDurableObject(scheduler, (_instance, state) => {
        state.storage.sql.exec("UPDATE scheduler_jobs SET status = ? WHERE job_id = ?", pendingStatus, actual.jobId);
      });
      await scheduler.workflowJobStarted({
        jobId: actual.jobId,
        runnerName: source.runnerName,
        target: legacyTarget,
        profile,
      });
      await expect(scheduler.claimProvisioning(actual.jobId)).resolves.toEqual({ kind: "cancelled" });
      expect((await scheduler.status()).slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(
        1,
      );
      await scheduler.provisioningFailed(actual.jobId, actual.runnerName, "cancelled own workflow");
      const status = await scheduler.status();
      expect(status.jobs.find((row) => row.jobId === actual.jobId)?.status).toBe("running");
      expect(status.slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(1);
    },
  );

  it("releases a cancelled provisioning reservation before any JIT registration", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("cancel-before-jit");
    const source = legacyJob("1500");
    await scheduler.submit(source);
    await expect(scheduler.claimProvisioning(source.jobId)).resolves.toMatchObject({ kind: "provision" });
    await scheduler.workflowJobCompleted(source.jobId);
    await expect(scheduler.canStart(source.jobId, source.runnerName)).resolves.toBe(false);
    await expect(scheduler.claimProvisioning(source.jobId)).resolves.toEqual({ kind: "cancelled" });
    const status = await scheduler.status();
    expect(status.jobs.find((row) => row.jobId === source.jobId)?.status).toBe("completed");
    expect(status.slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(0);
  });

  it("rejects a delayed start after stop so queued demand still gets a fresh runner", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("late-assignment");
    const source = legacyJob("1600");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 16_001);
    await runInDurableObject(scheduler, async (instance, state) => {
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockImplementation(async (_input, init) =>
          init?.method === "DELETE" ? new Response(null, { status: 204 }) : Response.json({ status: "queued" }),
        );
      try {
        await instance.runnerStopped(source.runnerName, { exitCode: 1, reason: "exit" });
        await expect(
          instance.workflowJobStarted({
            jobId: source.jobId,
            runnerName: source.runnerName,
            target: legacyTarget,
            profile,
          }),
        ).resolves.toMatchObject({ accepted: false });
        state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0 WHERE job_id = ?", source.jobId);
        await instance.alarm();
        expect(
          state.storage.sql.exec("SELECT runner_name FROM scheduler_jobs WHERE job_id = ?", source.jobId).toArray(),
        ).toEqual([{ runner_name: source.runnerName + "-r2" }]);
      } finally {
        fetch.mockRestore();
      }
    });
  });

  it("recovers GitHub demand for the actual job when a crossed runner stops", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("crossed-stop-demand");
    const source = legacyJob("1700");
    const actual = legacyJob("1701");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 17_001);
    await scheduler.submit(actual);
    await scheduler.workflowJobStarted({
      jobId: actual.jobId,
      runnerName: source.runnerName,
      target: legacyTarget,
      profile,
    });
    await scheduler.workflowJobCompleted(source.jobId);
    await runInDurableObject(scheduler, async (instance, state) => {
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockImplementation(async (_input, init) =>
          init?.method === "DELETE" ? new Response(null, { status: 204 }) : Response.json({ status: "queued" }),
        );
      try {
        await instance.runnerStopped(source.runnerName, { exitCode: 1, reason: "exit" });
        state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0 WHERE job_id = ?", actual.jobId);
        await instance.alarm();
        expect(
          state.storage.sql.exec("SELECT runner_name FROM scheduler_jobs WHERE job_id = ?", actual.jobId).toArray(),
        ).toEqual([{ runner_name: actual.runnerName + "-r2" }]);
        expect(
          state.storage.sql
            .exec("SELECT reserved FROM scheduler_jit_runners WHERE runner_name = ?", source.runnerName)
            .toArray(),
        ).toEqual([{ reserved: 0 }]);
      } finally {
        fetch.mockRestore();
      }
    });
  });

  it.each([
    { failure: "start failure", jobId: "2000" },
    { failure: "lost idle stop", jobId: "2001" },
  ])("reclaims $failure from runtime without a callback and retries queued demand", async ({ failure, jobId }) => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("runtime-stop-" + jobId);
    const source = legacyJob(jobId);
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, Number(jobId));
    if (failure === "start failure") {
      await scheduler.provisioningFailed(source.jobId, source.runnerName, "Container start threw");
    }
    await setRuntime(source.runnerName, "stopped");
    await runInDurableObject(scheduler, async (instance, state) => {
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockImplementation(async (_input, init) =>
          init?.method === "DELETE" ? new Response(null, { status: 204 }) : Response.json({ status: "queued" }),
        );
      try {
        await instance.alarm();
        expect(
          state.storage.sql
            .exec(
              "SELECT reserved, container_stop_reason FROM scheduler_jit_runners WHERE runner_name = ?",
              source.runnerName,
            )
            .toArray(),
        ).toEqual([{ reserved: 0, container_stop_reason: "runtime-not-running" }]);
        state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0 WHERE job_id = ?", source.jobId);
        await instance.alarm();
        await instance.provisioningFailed(source.jobId, source.runnerName, "late failure from old Workflow");
        expect(
          state.storage.sql
            .exec("SELECT runner_name, status FROM scheduler_jobs WHERE job_id = ?", source.jobId)
            .toArray(),
        ).toEqual([{ runner_name: source.runnerName + "-r2", status: "admitted" }]);
        expect(
          state.storage.sql
            .exec("SELECT reserved FROM scheduler_jit_runners WHERE runner_name = ?", source.runnerName + "-r2")
            .toArray(),
        ).toEqual([{ reserved: 1 }]);
      } finally {
        fetch.mockRestore();
      }
    });
  });

  it("retries the source demand when its crossed runner exits without onStop", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("crossed-runtime-stop");
    const source = legacyJob("2100");
    const actual = legacyJob("2101");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 21_000);
    await scheduler.submit(actual);
    await scheduler.workflowJobStarted({
      jobId: actual.jobId,
      runnerName: source.runnerName,
      target: legacyTarget,
      profile,
    });
    await scheduler.workflowJobCompleted(actual.jobId, source.runnerName);
    await setRuntime(source.runnerName, "stopped");
    await runInDurableObject(scheduler, async (instance, state) => {
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockImplementation(async (_input, init) =>
          init?.method === "DELETE" ? new Response(null, { status: 204 }) : Response.json({ status: "queued" }),
        );
      try {
        await instance.alarm();
        state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0 WHERE job_id = ?", source.jobId);
        await instance.alarm();
        expect(
          state.storage.sql
            .exec("SELECT runner_name, status FROM scheduler_jobs WHERE job_id = ?", source.jobId)
            .toArray(),
        ).toEqual([{ runner_name: source.runnerName + "-r2", status: "admitted" }]);
      } finally {
        fetch.mockRestore();
      }
    });
  });

  it("retries an expired admission with a fresh identity before any Workflow claims it", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("expired-admission");
    const source = legacyJob("2150");
    await scheduler.submit(source);
    await runInDurableObject(scheduler, async (instance, state) => {
      const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ status: "queued" }));
      try {
        state.storage.sql.exec("UPDATE scheduler_jit_runners SET provisioning_deadline = 0");
        await instance.alarm();
        expect((await instance.status()).jobs.find((row) => row.jobId === source.jobId)?.status).toBe(
          "stopped-awaiting-completion",
        );
        state.storage.sql.exec("UPDATE scheduler_jobs SET recovery_due_at = 0 WHERE job_id = ?", source.jobId);
        await instance.alarm();
        await expect(instance.claimProvisioning(source.jobId)).resolves.toMatchObject({
          kind: "provision",
          runnerName: source.runnerName + "-r2",
        });
      } finally {
        fetch.mockRestore();
      }
    });
  });

  it("records a crossed assignment before yielding to cancel its unused runner", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("stop-during-crossed-assignment");
    const source = legacyJob("2160");
    const actual = legacyJob("2161");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 21_600);
    await scheduler.submit(actual);
    await runInDurableObject(scheduler, async (instance) => {
      const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 204 }));
      try {
        const started = instance.workflowJobStarted({
          jobId: actual.jobId,
          runnerName: source.runnerName,
          target: legacyTarget,
          profile,
        });
        await instance.runnerStopped(source.runnerName, { exitCode: 1, reason: "exit during assignment" });
        await started;
        expect((await instance.status()).jobs.find((row) => row.jobId === actual.jobId)?.status).toBe(
          "stopped-awaiting-completion",
        );
      } finally {
        fetch.mockRestore();
      }
    });
  });

  it("rejects configuration and start acknowledgments from a retired provisioning attempt", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("stale-configuration-attempt");
    const source = legacyJob("2170");
    await scheduler.submit(source);
    await runInDurableObject(scheduler, async (instance, state) => {
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET status = 'provisioning', runner_name = ? WHERE job_id = ?",
        source.runnerName + "-r2",
        source.jobId,
      );
      state.storage.sql.exec(
        "UPDATE scheduler_slots SET configuration_state = 'configuring', configuration_owner_job_id = ? WHERE slot_id = 'preset:standard-3'",
        source.jobId,
      );
      await expect(instance.canStart(source.jobId, source.runnerName)).resolves.toBe(false);
      await expect(instance.configurationReady(source.jobId, source.runnerName, 0)).resolves.toEqual([]);
      expect(
        (await instance.status()).slots.find((slot) => slot.slotId === "preset:standard-3")?.configurationState,
      ).toBe("configuring");
    });
  });

  it("ignores an old failure when demand changes while that runner is being retired", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("failure-during-replacement");
    const source = legacyJob("2180");
    await scheduler.submit(source);
    await runInDurableObject(scheduler, async (instance, state) => {
      const failure = instance.provisioningFailed(source.jobId, source.runnerName, "old attempt failed");
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET status = 'provisioning', runner_name = ? WHERE job_id = ?",
        source.runnerName + "-r2",
        source.jobId,
      );
      await failure;
      expect((await instance.status()).jobs.find((row) => row.jobId === source.jobId)?.status).toBe("provisioning");
    });
  });

  it("keeps a running container after completion and retries failed runtime reads", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("runtime-live-and-unavailable");
    const source = legacyJob("2200");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 22_000);
    await scheduler.workflowJobStarted({
      jobId: source.jobId,
      runnerName: source.runnerName,
      target: legacyTarget,
      profile,
    });
    await scheduler.workflowJobCompleted(source.jobId, source.runnerName);
    await runInDurableObject(scheduler, async (instance, state) => {
      state.storage.sql.exec("UPDATE scheduler_jobs SET updated_at = 0 WHERE job_id = ?", source.jobId);
      await instance.alarm();
      expect((await instance.status()).slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(
        1,
      );
    });
    await setRuntime(source.runnerName, "error");
    await runInDurableObject(scheduler, (instance) => instance.alarm());
    const status = await scheduler.status();
    expect(status.slots.find((slot) => slot.slotId === "preset:standard-3")?.reservedCount).toBe(1);
    expect(status.events.some((event) => event.kind === "runner-runtime-reconciliation-retry")).toBe(true);
  });
});
