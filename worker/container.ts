// Durable Object wrapper around the container per Cloudflare's
// Containers pattern. Not end-to-end tested yet -- see the wrangler.toml
// header comment. This is the piece most likely to need adjustment once
// actually deployed against current Cloudflare SDK versions.
import { Container } from "@cloudflare/containers";

export class WhatsOnTheRiverAPI extends Container {
  defaultPort = 8080;
  sleepAfter = "10m"; // keep warm during active use, don't pay for idle
}
