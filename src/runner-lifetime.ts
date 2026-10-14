interface RetirementStore {
  retired(): boolean;
  retire(): void;
}

/** One runner identity may start until a stopped runtime observation retires it permanently. */
export class RunnerLifetime {
  private starting: Promise<void> | undefined;

  constructor(
    private readonly store: RetirementStore,
    private readonly isRunning: () => boolean,
  ) {}

  active(): boolean {
    return this.starting !== undefined || this.isRunning();
  }

  start(operation: () => Promise<void>): Promise<void> {
    if (this.store.retired()) {
      return Promise.reject(new Error("Runner reservation has expired"));
    }
    if (this.starting !== undefined) {
      return this.starting;
    }
    const starting = Promise.resolve().then(operation);
    this.starting = starting;
    return starting.finally(() => {
      if (this.starting === starting) {
        this.starting = undefined;
      }
    });
  }

  reconcile(provisioningDeadline: number): "starting" | "running" | "pending" | "stopped" {
    if (this.starting !== undefined) {
      return "starting";
    }
    if (this.isRunning()) {
      return "running";
    }
    if (Date.now() < provisioningDeadline) {
      return "pending";
    }
    this.store.retire();
    return "stopped";
  }
}
