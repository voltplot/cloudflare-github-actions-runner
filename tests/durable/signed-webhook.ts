export const webhookSecret = "public-test-fixture";

export async function signedWorkflowJobRequest(body: string): Promise<Request> {
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
