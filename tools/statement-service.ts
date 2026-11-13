// Runs the statement-service stub as its own process.
//
//   node tools/statement-service.ts [--port 4100] [--fail 503] [--fail-times 3]
//
// --fail makes it answer with that status (every request, or only the next --fail-times), to watch
// the worker retry and then compensate.
import { parseArgs } from "node:util";
import { startStatementService } from "../src/example/statement-service.ts";

const { values } = parseArgs({
  options: {
    port: { type: "string", default: "4100" },
    fail: { type: "string" },
    "fail-times": { type: "string" },
  },
});

const service = await startStatementService({ port: Number(values.port) });
if (values.fail) service.fail(Number(values.fail), values["fail-times"] ? Number(values["fail-times"]) : undefined);
console.log(`statement service listening on ${service.url}${values.fail ? `, failing with ${values.fail}` : ""}`);

let reported = 0;
setInterval(() => {
  if (service.requests === reported) return;
  reported = service.requests;
  console.log(JSON.stringify({ at: new Date().toISOString(), requests: service.requests, accepted: service.accepted.size }));
}, 1_000).unref();

for (const signal of ["SIGTERM", "SIGINT"] as const) process.once(signal, () => void service.close());
