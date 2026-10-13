import type { D1Migration } from "cloudflare:test";

import type { AccountRunnerScheduler } from "../../src/account-runner-scheduler";

declare module "cloudflare:test" {
  interface ProvidedEnv {
    RUNNER_SCHEDULER: DurableObjectNamespace<AccountRunnerScheduler>;
  }
}

declare module "vite-plus/test" {
  interface ProvidedContext {
    resourceMetricsMigrations: D1Migration[];
  }
}
