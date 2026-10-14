import { getContainer } from "@cloudflare/containers";

import type { WorkerEnvironment } from "./environment";

export function runnerContainerFor(env: WorkerEnvironment, slotId: string, runnerName: string) {
  const namespaces = new Map([
    ["preset:lite", env.RUNNER_LITE],
    ["preset:basic", env.RUNNER_BASIC],
    ["preset:standard-1", env.RUNNER_STANDARD_1],
    ["preset:standard-2", env.RUNNER_STANDARD_2],
    ["preset:standard-3", env.RUNNER_STANDARD_3],
    ["preset:standard-4", env.RUNNER_STANDARD_4],
    ["custom:1", env.RUNNER_CUSTOM],
    ["custom:2", env.RUNNER_CUSTOM_2],
    ["custom:3", env.RUNNER_CUSTOM_3],
    ["custom:4", env.RUNNER_CUSTOM_4],
    ["custom:5", env.RUNNER_CUSTOM_5],
    ["custom:6", env.RUNNER_CUSTOM_6],
    ["custom:7", env.RUNNER_CUSTOM_7],
    ["custom:8", env.RUNNER_CUSTOM_8],
    ["custom:9", env.RUNNER_CUSTOM_9],
    ["custom:10", env.RUNNER_CUSTOM_10],
  ]);
  const namespace = namespaces.get(slotId);
  if (namespace === undefined) {
    throw new Error(`Unknown Cloudflare runner application slot: ${slotId}`);
  }
  return getContainer(namespace, runnerName);
}
