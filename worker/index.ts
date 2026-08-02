// Thin Worker entrypoint that routes all requests into the Container
// (Cloudflare's container product runs behind a Durable Object). This
// file intentionally has zero business logic -- the actual API lives
// in the Python container (api/main.py, ingest/main.py). Verify this
// against current `wrangler containers` routing docs before deploy;
// written 2026-08-02 without an end-to-end test yet (blocked on DB).

export interface Env {
  API_CONTAINER: DurableObjectNamespace;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.API_CONTAINER.idFromName("singleton");
    const stub = env.API_CONTAINER.get(id);
    return stub.fetch(request);
  },
};

export { WhatsOnTheRiverAPI } from "./container";
