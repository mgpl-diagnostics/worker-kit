# @mgpl-diagnostics/worker-kit

Background jobs for the MGPL Diagnostics backend. You define a job once (name, processor, retries,
how long its result is kept) and start it from one queue API. Underneath, the jobs run:

- on **BullMQ over Redis**, with `startWorker` in a separate worker process (production);
- **in-process**, with no Redis (development, tests, a single server);
- or **on the worker when it is available, otherwise standalone** (`fallbackBackend`), the
  behaviour mail-kit relies on.

All three behave the same: the same JSON data, the same retries and backoff, the same states and the
same error messages. `npm test` runs one scenario on each and compares the results.

- The main entry has **no runtime dependencies**. `bullmq` and `ioredis` are optional peers, used only
  by the `/bullmq` entry.
- Job data is checked in the caller: it must be JSON, under a size limit, and free of secret-looking
  keys (`password`, `token`, `secret`, …). Payloads carry ids; the processor looks things up.
- Processor failures are safe to show: a `JobFailure` keeps its message, anything else is logged and
  shown as "Job failed".

## Install

```bash
# .npmrc: @mgpl-diagnostics:registry=https://npm.pkg.github.com
npm install @mgpl-diagnostics/worker-kit
npm install bullmq ioredis   # only for the /bullmq entry
```

## Define jobs

```ts
import { defineJob, JobFailure } from "@mgpl-diagnostics/worker-kit";

export const renderInvoice = defineJob<{ orgId: string; invoiceId: string }, { base64: string }>({
  name: "invoice",
  queue: "documents",        // default: the name
  attempts: 2,               // default 3, exponential backoff from backoffMs (default 2 s)
  keepResultsMs: 3600_000,   // finished jobs readable for an hour (default); 0 = removed at once
  validate: (d) => { if (!d.invoiceId) throw new Error("invoiceId is required"); },
  async process(d, ctx) {    // ctx: { id, name, queue, attempt, attempts }
    const invoice = await services.billing.find(d.orgId, d.invoiceId);
    if (!invoice) throw new JobFailure("Invoice not found", false); // shown to the client, no retry
    return { base64: await render(invoice) };                        // must be JSON
  },
});
```

Data and results cross JSON on every backend, so a `Date` arrives as an ISO string.

## Start and read jobs

```ts
import { createJobQueue, fallbackBackend, inlineBackend } from "@mgpl-diagnostics/worker-kit";
import { bullmqBackend } from "@mgpl-diagnostics/worker-kit/bullmq";

const backend = process.env.REDIS_URL
  ? bullmqBackend({ redisUrl: process.env.REDIS_URL, prefix: "mgpl" })
  : inlineBackend();               // { wait: false } runs jobs in the background of this process

const jobs = createJobQueue({ backend, jobs: [renderInvoice, passwordReset] });

const job = await jobs.enqueue(renderInvoice, { orgId, invoiceId });
// → { id, name, queue, state, attempts, error, result, data, createdAt, finishedAt }
const later = await jobs.get(renderInvoice, job.id); // null if unknown, expired, or another job's id
```

Job ids are random UUIDs, but they are not a permission: check that the reader owns the job
(compare `data` with the session) before showing it.

With `inlineBackend()` (the default `wait: true`), `enqueue` resolves when the job has finished, so
the request waits. With BullMQ it resolves at once with `state: "waiting"`.

## Run the worker

```ts
import { startWorker } from "@mgpl-diagnostics/worker-kit/bullmq";

const worker = startWorker({
  redisUrl: process.env.REDIS_URL!,
  prefix: "mgpl",
  jobs: [renderInvoice, passwordReset], // one BullMQ Worker per queue
  concurrency: 4,
  onError: (e, ctx) => log.error({ err: e, job: ctx }, "job failed"), // errors that are not a JobFailure
});

process.once("SIGTERM", () => worker.close()); // finishes the jobs in hand, then stops
```

A `JobFailure` with `retry: false` fails the job at once (BullMQ `UnrecoverableError`); every other
failure uses the remaining attempts.

## Worker when available, standalone otherwise

```ts
import { redisConnection } from "@mgpl-diagnostics/worker-kit/bullmq";

const connection = redisConnection(process.env.REDIS_URL!);
const jobs = createJobQueue({
  backend: fallbackBackend({
    primary: bullmqBackend({ connection, prefix: "mgpl" }), // available() = connection is "ready"
    fallback: inlineBackend(),
    onFallback: (reason, job) => log.warn({ reason, job: job.def.name }, "ran without the worker"),
    // whenFails: "throw" → WorkerError WORKER_UNAVAILABLE instead
  }),
  jobs: [mailJob],
});
```

### With mail-kit

`jobs.enqueuer(job)` has mail-kit's `MailQueue` shape, so mail-kit can queue through worker-kit:

```ts
const mailJob = defineJob<MailJob, SendResult>({ name: "mail", keepResultsMs: 0, attempts: 5, backoffMs: 10_000,
  process: (job) => mailer.deliver(job) });
const mailer = createMailer({ transport, from, queue: jobs.enqueuer(mailJob) });
```

`keepResultsMs: 0` removes a mail job as soon as it is sent, because it can hold a reset link.
In mail-kit's MailJob the link sits under `data.link`, which is allowed; a key named `token` would be refused.

## Errors

`WorkerError.code` is a public contract (add, never rename):

| Code | When |
| --- | --- |
| `WORKER_UNKNOWN_JOB` | the job is not in this queue's `jobs` (or is a different definition with the same name) |
| `WORKER_BAD_DATA` | data is not JSON, too large, has a secret-looking key, or `validate` threw; bad job/queue name |
| `WORKER_DUPLICATE_JOB` | two definitions share a name |
| `WORKER_UNAVAILABLE` | `fallbackBackend` with `whenFails: "throw"`; an in-process `enqueuer` job failed; no Redis given |
| `WORKER_CLOSED` | the queue was closed |

## Develop

```bash
npm run typecheck && npm test && npm run build
npm run llms   # regenerate llms-full.txt (CI fails when it is stale)
```

`npm test` uses `REDIS_URL`, or starts a local `redis-server` when one is installed; without either the
BullMQ checks are skipped (CI sets `REQUIRE_REDIS=1` and a Redis service, so they always run there).

## Licence

All rights reserved (see [LICENSE](LICENSE)).
