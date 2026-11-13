// A stand-in for an external statement service. It accepts each batch once per idempotency key and
// can be told to fail, so tests and demos can see retries, duplicates and compensation.
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";

export interface StatementService {
  url: string;
  /** Every request received, including failed and repeated ones. */
  readonly requests: number;
  /** Batches accepted, by idempotency key. A repeat with the same key is not accepted again. */
  readonly accepted: ReadonlyMap<string, string>;
  /** Answer the next `times` requests (or every request, if left out) with this status. */
  fail(status: number, times?: number): void;
  /** Go back to answering normally. */
  heal(): void;
  close(): Promise<void>;
}

export async function startStatementService(options: { port?: number; host?: string } = {}): Promise<StatementService> {
  const accepted = new Map<string, string>();
  let requests = 0;
  let failure: { status: number; remaining: number } | undefined;

  const server = createServer(async (req, res) => {
    const body = await readBody(req);
    requests++;
    const reply = (status: number, payload: object) => {
      res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(payload));
    };

    if (req.method !== "POST" || req.url !== "/statements") return reply(404, { error: "not found" });
    if (failure && failure.remaining > 0) {
      failure.remaining--;
      return reply(failure.status, { error: "failing on purpose" });
    }
    const key = req.headers["idempotency-key"];
    if (typeof key !== "string" || key === "") return reply(400, { error: "an Idempotency-Key header is required" });

    const earlier = accepted.get(key);
    if (earlier === undefined) {
      accepted.set(key, body);
      return reply(201, { status: "accepted" });
    }
    // The same key with a different body is a client bug, not a retry.
    if (earlier !== body) return reply(409, { error: "this key was already used for a different batch" });
    return reply(200, { status: "already accepted" });
  });

  await new Promise<void>((resolve) => server.listen(options.port ?? 0, options.host ?? "127.0.0.1", resolve));
  const { address, port } = server.address() as AddressInfo;

  return {
    url: `http://${address}:${port}`,
    get requests() {
      return requests;
    },
    accepted,
    fail(status, times = Infinity) {
      failure = { status, remaining: times };
    },
    heal() {
      failure = undefined;
    },
    close() {
      server.closeAllConnections();
      return new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    },
  };
}

async function readBody(req: IncomingMessage): Promise<string> {
  const parts: Buffer[] = [];
  for await (const part of req) parts.push(part as Buffer);
  return Buffer.concat(parts).toString("utf8");
}
