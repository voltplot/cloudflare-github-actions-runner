import { describe, expect, it, vi } from "vite-plus/test";

import { RunnerLifetime } from "../src/runner-lifetime";

function retirementStore() {
  let retired = false;
  return {
    retired: () => retired,
    retire: () => {
      retired = true;
    },
  };
}

describe("runner runtime lifetime", () => {
  it("keeps a running container regardless of the provisioning deadline", () => {
    const store = retirementStore();
    let running = true;
    const lifetime = new RunnerLifetime(store, () => running);
    expect(lifetime.reconcile(0)).toBe("running");
    expect(store.retired()).toBe(false);
    running = false;
    expect(lifetime.reconcile(0)).toBe("stopped");
    expect(store.retired()).toBe(true);
  });

  it("protects and coalesces startup before the runtime becomes running", async () => {
    const store = retirementStore();
    const lifetime = new RunnerLifetime(store, () => false);
    const pending = new Promise<void>((resolve) => queueMicrotask(resolve));
    const start = vi.fn<() => Promise<void>>(() => pending);
    const first = lifetime.start(start);
    const second = lifetime.start(start);
    expect(lifetime.active()).toBe(true);
    expect(lifetime.reconcile(0)).toBe("starting");
    expect(store.retired()).toBe(false);
    await Promise.all([first, second]);
    expect(start).toHaveBeenCalledTimes(1);
    expect(lifetime.reconcile(0)).toBe("stopped");
  });

  it("retires a failed start and prevents delayed starts after a restart", async () => {
    const store = retirementStore();
    const lifetime = new RunnerLifetime(store, () => false);
    await expect(
      lifetime.start(async () => {
        throw new Error("start failed");
      }),
    ).rejects.toThrow("start failed");
    expect(lifetime.reconcile(0)).toBe("stopped");
    const restarted = new RunnerLifetime(store, () => false);
    const lateStart = vi.fn<() => Promise<void>>(async () => {});
    await expect(restarted.start(lateStart)).rejects.toThrow("Runner reservation has expired");
    expect(lateStart).not.toHaveBeenCalled();
  });

  it("allows provisioning before its deadline without retiring the identity", () => {
    const store = retirementStore();
    const lifetime = new RunnerLifetime(store, () => false);
    expect(lifetime.reconcile(Date.now() + 60_000)).toBe("pending");
    expect(store.retired()).toBe(false);
    expect(lifetime.reconcile(0)).toBe("stopped");
  });
});
