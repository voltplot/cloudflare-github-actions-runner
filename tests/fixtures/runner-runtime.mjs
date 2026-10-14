import { DurableObject } from "cloudflare:workers";

// Scripted scheduler peer: this exercises real RPC/storage, not the Containers engine or production startup fence.
export class RunnerRuntimeFixture extends DurableObject {
  reconcile(provisioningDeadline) {
    const runtime = this.ctx.storage.kv.get("runtime") ?? "stopped";
    if (runtime === "error") {
      throw new Error("Scripted runner runtime unavailable");
    }
    if (runtime === "running" || runtime === "starting") {
      return runtime;
    }
    if (Date.now() < provisioningDeadline) {
      return "pending";
    }
    this.ctx.storage.kv.put("retired", true);
    return "stopped";
  }

  fetch(request) {
    const runtime = new URL(request.url).pathname.slice(1);
    if (request.method === "POST" && ["running", "starting", "stopped", "error"].includes(runtime)) {
      this.ctx.storage.kv.put("runtime", runtime);
      return new Response(null, { status: 204 });
    }
    if (request.method === "GET") {
      return Response.json({
        runtime: this.ctx.storage.kv.get("runtime") ?? "stopped",
        retired: this.ctx.storage.kv.get("retired") === true,
      });
    }
    return new Response("Unknown runtime fixture command", { status: 400 });
  }
}
