import { createExecutionContext, env, runInDurableObject, waitOnExecutionContext } from "cloudflare:test";
import { expect, it } from "vite-plus/test";
import { z } from "zod";

import type { WorkerEnvironment } from "../../src/environment";
import worker from "../../src/index";

const token = "scheduler-status-test-token";
const eventsSchema = z.object({
  events: z.array(z.object({ createdAt: z.number(), kind: z.string() })),
});

function statusRequest(since?: string): Request {
  const url = new URL("https://runner.example.workers.dev/v1/scheduler/status");
  if (since !== undefined) {
    url.searchParams.set("since", since);
  }
  return new Request(url, { headers: { Authorization: `Bearer ${token}` } });
}

async function fetchStatus(account: string, since?: string): Promise<Response> {
  const ctx = createExecutionContext();
  const response = await worker.fetch(
    statusRequest(since),
    // SAFETY: This endpoint reads only the scheduler binding and setup token set here.
    { ...(env as WorkerEnvironment), CLOUDFLARE_ACCOUNT_ID: account, RUNNER_SETUP_VALIDATION_TOKEN: token },
    ctx,
  );
  await waitOnExecutionContext(ctx);
  return response;
}

it("returns all scheduler events since a timestamp while the default stays at 100", async () => {
  const account = `scheduler-status-${crypto.randomUUID()}`;
  const scheduler = env.RUNNER_SCHEDULER.getByName(account);
  await scheduler.status();

  const firstEventAt = 1_700_000_000_000;
  await runInDurableObject(scheduler, (_instance, state) => {
    for (let index = 0; index < 105; index += 1) {
      state.storage.sql.exec(
        "INSERT INTO scheduler_events (job_id, slot_id, kind, detail_json, created_at) VALUES (NULL, NULL, ?, NULL, ?)",
        `test-${index}`,
        firstEventAt + index,
      );
    }
  });

  const defaultResponse = await fetchStatus(account);
  expect(defaultResponse.status).toBe(200);
  const defaultStatus = eventsSchema.parse(await defaultResponse.json());
  expect(defaultStatus.events).toHaveLength(100);
  expect(defaultStatus.events[0]?.createdAt).toBe(firstEventAt + 104);
  expect(defaultStatus.events.at(-1)?.createdAt).toBe(firstEventAt + 5);

  const historyResponse = await fetchStatus(account, String(firstEventAt));
  expect(historyResponse.status).toBe(200);
  const history = eventsSchema.parse(await historyResponse.json());
  expect(history.events).toHaveLength(105);
  expect(history.events.map((event) => event.createdAt)).toEqual(
    Array.from({ length: 105 }, (_, index) => firstEventAt + 104 - index),
  );

  const boundaryResponse = await fetchStatus(account, String(firstEventAt + 104));
  const boundary = eventsSchema.parse(await boundaryResponse.json());
  expect(boundary.events.map((event) => event.createdAt)).toEqual([firstEventAt + 104]);
});

it("checks setup authorization before validating since and rejects invalid timestamps", async () => {
  const account = `scheduler-status-auth-${crypto.randomUUID()}`;
  const unauthorizedContext = createExecutionContext();
  const unauthorizedResponse = await worker.fetch(
    new Request("https://runner.example.workers.dev/v1/scheduler/status?since=", { method: "GET" }),
    // SAFETY: The request must stop at setup authorization before reading any scheduler binding.
    { ...(env as WorkerEnvironment), CLOUDFLARE_ACCOUNT_ID: account, RUNNER_SETUP_VALIDATION_TOKEN: token },
    unauthorizedContext,
  );
  await waitOnExecutionContext(unauthorizedContext);
  expect(unauthorizedResponse.status).toBe(401);

  const invalid = ["", "not-a-number", "Infinity", "-1", "1.5"];
  const responses = await Promise.all(invalid.map((since) => fetchStatus(account, since)));
  for (const [index, response] of responses.entries()) {
    expect(response.status, `since=${JSON.stringify(invalid[index])}`).toBe(400);
  }
});
