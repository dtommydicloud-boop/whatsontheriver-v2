// Durable Object wrapper around the container per Cloudflare's
// Containers pattern.
//
// Real bug found deploying this 2026-08-02: a Worker secret set via
// `wrangler secret put` lives in the WORKER's environment, not the
// container's -- it doesn't cross that boundary automatically. Without
// `envVars` set here, the container process starts with no DATABASE_URL,
// api/main.py's asyncpg.create_pool() throws on startup, and the
// container never becomes healthy (confirmed via `wrangler containers
// instances` showing state "inactive" even though the image deployed
// fine) -- looked identical to a networking/port problem from the
// outside, but was actually a config-plumbing gap.
import { Container } from "@cloudflare/containers";

interface Env {
  DATABASE_URL: string;
  SOURCE_SECRET_lake_mac: string;
  SOURCE_SECRET_red_wing: string;
}

export class WhatsOnTheRiverAPI extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "10m"; // keep warm during active use, don't pay for idle

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.envVars = {
      DATABASE_URL: env.DATABASE_URL,
      SOURCE_SECRET_lake_mac: env.SOURCE_SECRET_lake_mac,
      SOURCE_SECRET_red_wing: env.SOURCE_SECRET_red_wing,
    };
  }
}
