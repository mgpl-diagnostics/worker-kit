// worker-kit checks: definitions, data rules, the in-process backend, fallback, the producer API,
// and BullMQ on a real Redis, where the same scenario must give the same results as in-process.
// Redis: REDIS_URL, or a local `redis-server` started here; CI sets REQUIRE_REDIS=1 so it never skips.
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createServer } from "node:net";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { bullJobOptions, bullmqBackend, redisConnection, startWorker } from "../src/bullmq";
import {
  createJobQueue,
  defineJob,
  fallbackBackend,
  inlineBackend,
  isJobFailure,
  JobFailure,
  prepareJob,
  WorkerError,
  type AnyJob,
  type JobBackend,
  type JobContext,
  type JobInfo,
} from "../src/index";

let passed = 0;
const failures: string[] = [];
async function check(name: string, fn: () => unknown): Promise<void> {
  try {
    await fn();
    passed++;
  } catch (e) {
    failures.push(`✗ ${name}\n    ${(e as Error).stack?.split("\n").slice(0, 4).join("\n    ")}`);
  }
}
const code = async (fn: () => unknown, expected: string) => {
  try {
    await fn();
  } catch (e) {
    assert.ok(e instanceof WorkerError, `expected WorkerError, got ${String(e)}`);
    assert.equal(e.code, expected, e.message);
    return e.message;
  }
  assert.fail(`expected ${expected}`);
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitFor<T>(fn: () => Promise<T | null | undefined | false>, ms = 10_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > end) assert.fail("timed out");
    await sleep(20);
  }
}
const quiet = () => undefined;

// ---- the scenario every backend must run identically --------------------------------------------

interface SumData {
  a: number;
  b: number;
  at: Date | string;
}
function scenario() {
  const errors: string[] = [];
  const calls: Record<string, number[]> = {};
  const seen = (ctx: JobContext) => (calls[ctx.name] ??= []).push(ctx.attempt);
  const jobs = {
    sum: defineJob<SumData, { sum: number; at: unknown; when: Date }>({
      name: "sum",
      queue: "maths",
      process: (d, ctx) => {
        seen(ctx);
        return { sum: d.a + d.b, at: d.at, when: new Date(0) };
      },
    }),
    flaky: defineJob<{ n: number }, string>({
      name: "flaky",
      queue: "maths",
      attempts: 3,
      backoffMs: 10,
      process: (d, ctx) => {
        seen(ctx);
        if (ctx.attempt < 3) throw new JobFailure("Busy, try again", true);
        return `ok ${d.n} on ${ctx.attempt}/${ctx.attempts}`;
      },
    }),
    fatal: defineJob<{ invoiceId: string }, never>({
      name: "fatal",
      attempts: 5,
      backoffMs: 10,
      process: (d, ctx) => {
        seen(ctx);
        throw new JobFailure(`No invoice ${d.invoiceId}`, false);
      },
    }),
    crash: defineJob<{ id: string }, never>({
      name: "crash",
      attempts: 2,
      backoffMs: 10,
      process: (_d, ctx) => {
        seen(ctx);
        throw new Error("connect ECONNREFUSED db.internal:5432 user=admin");
      },
    }),
  };
  const onError = (e: unknown) => errors.push((e as Error).message);
  return { jobs, list: Object.values(jobs) as AnyJob[], errors, calls, onError };
}

/** Runs the scenario on a backend; returns comparable results. */
async function runScenario(backend: JobBackend, s: ReturnType<typeof scenario>) {
  const q = createJobQueue({ backend, jobs: s.list });
  const started = [
    await q.enqueue(s.jobs.sum, { a: 2, b: 3, at: new Date("2026-10-14T10:00:00.000Z") }),
    await q.enqueue(s.jobs.flaky, { n: 7 }),
    await q.enqueue(s.jobs.fatal, { invoiceId: "INV-1" }),
    await q.enqueue(s.jobs.crash, { id: "x" }),
  ];
  const done: JobInfo[] = [];
  for (const j of started) {
    const def = q.definition(j.name);
    done.push(await waitFor(async () => {
      const info = await q.get(def, j.id);
      return info && (info.state === "completed" || info.state === "failed") ? info : null;
    }));
  }
  return { q, done: done.map(({ id: _id, createdAt, finishedAt, ...rest }) => ({ ...rest, timed: createdAt instanceof Date && finishedAt instanceof Date && finishedAt >= createdAt })) };
}

const expected = [
  { name: "sum", queue: "maths", state: "completed", attempts: 1, error: null, result: { sum: 5, at: "2026-10-14T10:00:00.000Z", when: "1970-01-01T00:00:00.000Z" }, data: { a: 2, b: 3, at: "2026-10-14T10:00:00.000Z" }, timed: true },
  { name: "flaky", queue: "maths", state: "completed", attempts: 3, error: null, result: "ok 7 on 3/3", data: { n: 7 }, timed: true },
  { name: "fatal", queue: "fatal", state: "failed", attempts: 1, error: "No invoice INV-1", result: null, data: { invoiceId: "INV-1" }, timed: true },
  { name: "crash", queue: "crash", state: "failed", attempts: 2, error: "Job failed", result: null, data: { id: "x" }, timed: true },
];

// ---- definitions and data rules -----------------------------------------------------------------

await check("defineJob: defaults, queue = name, frozen", () => {
  const j = defineJob({ name: "mail", process: () => 1 });
  assert.equal(j.queue, "mail");
  assert.equal(j.attempts, 3);
  assert.equal(j.backoffMs, 2000);
  assert.equal(j.keepResultsMs, 3600_000);
  assert.equal(j.keepFailedMs, 86_400_000);
  assert.equal(j.refuseSecrets, true);
  assert.ok(Object.isFrozen(j));
  assert.equal(defineJob({ name: "x", keepResultsMs: 2 * 86_400_000, process: () => 1 }).keepFailedMs, 2 * 86_400_000);
  assert.equal(defineJob({ name: "x", attempts: 0, process: () => 1 }).attempts, 1);
});

await check("defineJob: names are checked (they become Redis keys)", async () => {
  await code(() => defineJob({ name: "Bad Name", process: () => 1 }), "WORKER_BAD_DATA");
  await code(() => defineJob({ name: "ok", queue: "a:b", process: () => 1 }), "WORKER_BAD_DATA");
  await code(() => defineJob({ name: "", process: () => 1 }), "WORKER_BAD_DATA");
});

await check("data crosses JSON: Dates become ISO strings, undefined keys drop", () => {
  const j = defineJob<Record<string, unknown>, null>({ name: "j", process: () => null });
  assert.deepEqual(prepareJob(j, { at: new Date(0), gone: undefined, n: 1 }).data, { at: "1970-01-01T00:00:00.000Z", n: 1 });
});

await check("data that is not JSON, too large, or carries secrets is refused in the caller", async () => {
  const j = defineJob<unknown, null>({ name: "j", maxBytes: 100, process: () => null });
  await code(() => prepareJob(j, { n: 1n }), "WORKER_BAD_DATA");
  await code(() => prepareJob(j, undefined), "WORKER_BAD_DATA");
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  await code(() => prepareJob(j, cyclic), "WORKER_BAD_DATA");
  await code(() => prepareJob(j, { s: "x".repeat(200) }), "WORKER_BAD_DATA");
  const m = await code(() => prepareJob(j, { user: { id: "u1", resetToken: "abc" } }), "WORKER_BAD_DATA");
  assert.match(m, /user\.resetToken/);
  assert.doesNotMatch(m, /abc/);
  await code(() => prepareJob(j, { list: [{ PASSWORD: "x" }] }), "WORKER_BAD_DATA");
  await code(() => prepareJob(j, { headers: { authorization: "Bearer x" } }), "WORKER_BAD_DATA");
  // ids and links are fine; opting out is explicit
  prepareJob(j, { userId: "u1", tokenId: "t1", link: "https://x.example/r" });
  prepareJob(defineJob<unknown, null>({ name: "k", refuseSecrets: false, process: () => null }), { token: "x" });
});

await check("validate() runs before enqueueing; its error becomes WORKER_BAD_DATA", async () => {
  let ran = false;
  const j = defineJob<{ email: string }, null>({
    name: "j",
    validate: (d) => {
      if (!d.email.includes("@")) throw new Error("email is not valid");
    },
    process: () => {
      ran = true;
      return null;
    },
  });
  const q = createJobQueue({ backend: inlineBackend(), jobs: [j] });
  assert.match(await code(() => q.enqueue(j, { email: "nope" }), "WORKER_BAD_DATA"), /email is not valid/);
  assert.equal(ran, false);
  assert.equal((await q.enqueue(j, { email: "a@example.com" })).state, "completed");
});

await check("every job id is a fresh UUID", () => {
  const j = defineJob({ name: "j", process: () => 1 });
  const ids = new Set(Array.from({ length: 50 }, () => prepareJob(j, {}).id));
  assert.equal(ids.size, 50);
  for (const id of ids) assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
});

await check("JobFailure is recognised across package copies", () => {
  const foreign = Object.assign(new Error("x"), { name: "JobFailure", retry: false });
  assert.ok(isJobFailure(foreign));
  assert.ok(isJobFailure(new JobFailure("y")));
  assert.equal(new JobFailure("y").retry, true);
  assert.ok(!isJobFailure(new Error("z")));
});

// ---- the producer API ---------------------------------------------------------------------------

await check("createJobQueue: unknown jobs, duplicate names, foreign definitions", async () => {
  const a = defineJob({ name: "a", process: () => 1 });
  await code(() => createJobQueue({ backend: inlineBackend(), jobs: [a, defineJob({ name: "a", process: () => 2 })] }), "WORKER_DUPLICATE_JOB");
  const q = createJobQueue({ backend: inlineBackend(), jobs: [a] });
  assert.deepEqual(q.names(), ["a"]);
  assert.equal(q.mode, "inline");
  await code(() => q.enqueue("b", {}), "WORKER_UNKNOWN_JOB");
  await code(() => q.enqueue(defineJob({ name: "a", process: () => 3 }), {}), "WORKER_UNKNOWN_JOB");
  assert.equal((await q.enqueue("a", {})).result, 1);
  assert.equal(await q.get(a, ""), null);
});

await check("get only returns a job under its own definition", async () => {
  const a = defineJob({ name: "a", queue: "shared", process: () => "A" });
  const b = defineJob({ name: "b", queue: "shared", process: () => "B" });
  const q = createJobQueue({ backend: inlineBackend(), jobs: [a, b] });
  const job = await q.enqueue(a, {});
  assert.equal((await q.get(a, job.id))?.result, "A");
  assert.equal(await q.get(b, job.id), null);
  assert.equal(await q.get(a, "00000000-0000-4000-8000-000000000000"), null);
});

await check("close: enqueue refused afterwards, available() false", async () => {
  const a = defineJob({ name: "a", process: () => 1 });
  const q = createJobQueue({ backend: inlineBackend(), jobs: [a] });
  assert.equal(await q.available(), true);
  await q.close();
  await q.close();
  assert.equal(await q.available(), false);
  await code(() => q.enqueue(a, {}), "WORKER_CLOSED");
});

// ---- in-process ---------------------------------------------------------------------------------

await check("inline (waiting): the scenario", async () => {
  const s = scenario();
  const { done } = await runScenario(inlineBackend({ onError: s.onError }), s);
  assert.deepEqual(done, expected);
  assert.deepEqual(s.calls, { sum: [1], flaky: [1, 2, 3], fatal: [1], crash: [1, 2] });
  assert.equal(s.errors.length, 2);
});

await check("inline (background): the scenario, with backoff between attempts", async () => {
  const s = scenario();
  const t0 = Date.now();
  const { done } = await runScenario(inlineBackend({ wait: false, onError: s.onError }), s);
  assert.deepEqual(done, expected);
  assert.ok(Date.now() - t0 >= 30, "flaky waited 10 + 20 ms");
});

await check("inline (background): add resolves before the job runs", async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const j = defineJob({ name: "slow", process: async () => (await gate, "done") });
  const q = createJobQueue({ backend: inlineBackend({ wait: false }), jobs: [j] });
  const info = await q.enqueue(j, {});
  assert.equal(info.state, "waiting");
  await sleep(5);
  assert.equal((await q.get(j, info.id))?.state, "active");
  release();
  assert.equal((await waitFor(async () => ((await q.get(j, info.id))?.state === "completed" ? q.get(j, info.id) : null)))?.result, "done");
});

await check("inline: errors that are not JobFailure are hidden and passed to onError", async () => {
  const logged: unknown[] = [];
  const j = defineJob({ name: "j", attempts: 1, process: () => Promise.reject(new Error("secret internals")) });
  const q = createJobQueue({ backend: inlineBackend({ onError: (e, ctx) => logged.push([(e as Error).message, ctx.name, ctx.attempt]) }), jobs: [j] });
  const info = await q.enqueue(j, {});
  assert.equal(info.state, "failed");
  assert.equal(info.error, "Job failed");
  assert.deepEqual(logged, [["secret internals", "j", 1]]);
});

await check("inline: a result that is not JSON fails without retrying", async () => {
  let runs = 0;
  const j = defineJob({ name: "j", process: () => (runs++, { n: 1n }) });
  const info = await createJobQueue({ backend: inlineBackend({ onError: quiet }), jobs: [j] }).enqueue(j, {});
  assert.equal(info.state, "failed");
  assert.equal(runs, 1);
});

await check("inline: delayed jobs wait, then run", async () => {
  const j = defineJob({ name: "later", process: () => "ran" });
  const q = createJobQueue({ backend: inlineBackend(), jobs: [j] });
  const info = await q.enqueue(j, {}, { delayMs: 40 });
  assert.equal(info.state, "delayed");
  await sleep(10);
  assert.equal((await q.get(j, info.id))?.state, "delayed");
  const done = await waitFor(async () => ((await q.get(j, info.id))?.state === "completed" ? q.get(j, info.id) : null));
  assert.equal(done?.result, "ran");
});

await check("inline: results are kept for keepResultsMs, failures for keepFailedMs", async () => {
  const gone = defineJob({ name: "gone", keepResultsMs: 0, process: () => "x" });
  const brief = defineJob({ name: "brief", keepResultsMs: 30, process: () => "y" });
  const bad = defineJob({ name: "bad", attempts: 1, keepFailedMs: 30, process: () => Promise.reject(new JobFailure("no", false)) });
  const q = createJobQueue({ backend: inlineBackend(), jobs: [gone, brief, bad] });
  const g = await q.enqueue(gone, {});
  assert.equal(g.result, "x", "the caller still sees the outcome");
  assert.equal(await q.get(gone, g.id), null);
  const b = await q.enqueue(brief, {});
  const f = await q.enqueue(bad, {});
  assert.equal((await q.get(brief, b.id))?.result, "y");
  assert.equal((await q.get(bad, f.id))?.error, "no");
  await sleep(40);
  assert.equal(await q.get(brief, b.id), null);
  assert.equal(await q.get(bad, f.id), null);
});

await check("inline: close finishes jobs in hand and drops pending delays", async () => {
  let finished = false;
  let delayedRan = false;
  const slow = defineJob({ name: "slow", process: async () => (await sleep(30), (finished = true)) });
  const later = defineJob({ name: "later", process: () => (delayedRan = true) });
  const q = createJobQueue({ backend: inlineBackend({ wait: false }), jobs: [slow, later] });
  await q.enqueue(slow, {});
  await q.enqueue(later, {}, { delayMs: 60_000 });
  await sleep(5);
  const t0 = Date.now();
  await q.close();
  assert.ok(finished);
  assert.ok(!delayedRan);
  assert.ok(Date.now() - t0 < 1000, "did not wait for the delay");
});

// ---- fallback -----------------------------------------------------------------------------------

function fakeBackend(o: { available?: boolean; fails?: boolean }) {
  const added: string[] = [];
  const inner = inlineBackend();
  const b: JobBackend = {
    mode: "fake",
    async add(job) {
      if (o.fails) throw new Error("redis went away");
      added.push(job.def.name);
      return inner.add(job);
    },
    get: (def, id) => inner.get(def, id),
    available: () => o.available ?? true,
    close: () => inner.close(),
  };
  return { b, added };
}

await check("fallback: the primary when available", async () => {
  const p = fakeBackend({});
  const fellBack: unknown[] = [];
  const j = defineJob({ name: "j", process: () => 1 });
  const q = createJobQueue({ backend: fallbackBackend({ primary: p.b, fallback: inlineBackend(), onFallback: (r) => fellBack.push(r) }), jobs: [j] });
  const info = await q.enqueue(j, {});
  assert.deepEqual(p.added, ["j"]);
  assert.equal(fellBack.length, 0);
  assert.equal((await q.get(j, info.id))?.result, 1);
  assert.equal(q.mode, "fallback");
});

await check("fallback: standalone when the primary is unavailable or fails", async () => {
  for (const o of [{ available: false }, { fails: true }]) {
    const p = fakeBackend(o);
    const reasons: string[] = [];
    const j = defineJob({ name: "j", process: () => 2 });
    const q = createJobQueue({ backend: fallbackBackend({ primary: p.b, fallback: inlineBackend(), onFallback: (r) => reasons.push((r as Error).message) }), jobs: [j] });
    const info = await q.enqueue(j, {});
    assert.equal(info.result, 2);
    assert.deepEqual(p.added, []);
    assert.equal(reasons.length, 1);
    assert.equal((await q.get(j, info.id))?.result, 2, "found on the fallback");
    assert.equal(await q.available(), true);
  }
});

await check("fallback: whenFails 'throw' raises WORKER_UNAVAILABLE", async () => {
  const j = defineJob({ name: "j", process: () => 1 });
  for (const o of [{ available: false }, { fails: true }]) {
    const q = createJobQueue({ backend: fallbackBackend({ primary: fakeBackend(o).b, fallback: inlineBackend(), whenFails: "throw" }), jobs: [j] });
    await code(() => q.enqueue(j, {}), "WORKER_UNAVAILABLE");
  }
});

await check("enqueuer: mail-kit's MailQueue shape", async () => {
  // mail-kit: interface MailQueue { enqueue(job): Promise<string | undefined>; available?(): boolean | Promise<boolean> }
  interface MailQueueShape {
    enqueue(job: { v: 1; template: string }): Promise<string | undefined>;
    available?(): boolean | Promise<boolean>;
  }
  const sent: string[] = [];
  const mail = defineJob<{ v: 1; template: string }, null>({ name: "mail", keepResultsMs: 0, process: (d) => (sent.push(d.template), null) });
  const broken = defineJob<{ v: 1; template: string }, null>({ name: "broken", attempts: 1, process: () => Promise.reject(new JobFailure("SMTP refused", false)) });
  const q = createJobQueue({ backend: inlineBackend(), jobs: [mail, broken] });
  const mq: MailQueueShape = q.enqueuer(mail);
  const id = await mq.enqueue({ v: 1, template: "password-reset" });
  assert.equal(typeof id, "string");
  assert.deepEqual(sent, ["password-reset"]);
  assert.equal(await mq.available?.(), true);
  await assert.rejects(q.enqueuer(broken).enqueue({ v: 1, template: "x" }), /SMTP refused/);
});

// ---- BullMQ on Redis ----------------------------------------------------------------------------

async function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const s = createServer().listen(0, "127.0.0.1", () => {
      const port = (s.address() as { port: number }).port;
      s.close(() => resolve(port));
    });
  });
}

let redisUrl = process.env.REDIS_URL;
let redisServer: ChildProcess | undefined;
if (!redisUrl && spawnSync("redis-server", ["--version"]).status === 0) {
  const port = await freePort();
  redisServer = spawn("redis-server", ["--port", String(port), "--save", "", "--appendonly", "no"], { stdio: "ignore" });
  redisUrl = `redis://127.0.0.1:${port}`;
  const probe = new Redis(redisUrl, { lazyConnect: true, maxRetriesPerRequest: 0, retryStrategy: () => 50 });
  probe.on("error", quiet);
  await waitFor(async () => (await probe.connect().then(() => true, () => false)) && (await probe.ping()) === "PONG", 5000);
  probe.disconnect();
}

if (!redisUrl) {
  if (process.env.REQUIRE_REDIS) failures.push("✗ BullMQ checks: no Redis (set REDIS_URL or install redis-server)");
  else console.log("- BullMQ checks skipped: no REDIS_URL and no redis-server");
} else {
  const prefix = `wk-test-${process.pid}`;
  const admin = new Redis(redisUrl);

  await check("bullmq: the scenario gives the same results as in-process", async () => {
    const s = scenario();
    const worker = startWorker({ redisUrl, prefix, jobs: s.list, concurrency: 2, onError: s.onError, onFailed: quiet });
    assert.deepEqual(worker.queues.sort(), ["crash", "fatal", "maths"]);
    const backend = bullmqBackend({ redisUrl, prefix });
    await waitFor(async () => backend.available?.());
    const { q, done } = await runScenario(backend, s);
    assert.equal(q.mode, "bullmq");
    assert.deepEqual(done, expected);
    assert.deepEqual(s.calls, { sum: [1], flaky: [1, 2, 3], fatal: [1], crash: [1, 2] });
    assert.equal(s.errors.length, 2);
    await worker.close();
    await q.close();
  });

  await check("bullmq: keys use the prefix and queue; job options come from the definition", async () => {
    const j = defineJob({ name: "keys", queue: "kq", attempts: 4, backoffMs: 500, keepResultsMs: 60_000, keepFailedMs: 120_000, process: () => 1 });
    const prepared = prepareJob(j, { id: "a" }, { delayMs: 1000 });
    assert.deepEqual(bullJobOptions(prepared), { jobId: prepared.id, attempts: 4, backoff: { type: "exponential", delay: 500 }, delay: 1000, removeOnComplete: { age: 60 }, removeOnFail: { age: 120 } });
    const q = createJobQueue({ backend: bullmqBackend({ redisUrl, prefix }), jobs: [j] });
    const info = await q.enqueue(j, { id: "a" }, { delayMs: 60_000 });
    assert.equal(info.state, "delayed");
    assert.equal((await q.get(j, info.id))?.state, "delayed");
    assert.ok((await admin.keys(`${prefix}:kq:*`)).length > 0);
    await q.close();
  });

  await check("bullmq: keepResultsMs 0 removes the job (and its data) once done", async () => {
    const j = defineJob({ name: "mail", keepResultsMs: 0, process: () => "sent" });
    const worker = startWorker({ redisUrl, prefix, jobs: [j] });
    const q = createJobQueue({ backend: bullmqBackend({ redisUrl, prefix }), jobs: [j] });
    const info = await q.enqueue(j, { link: "https://x.example/reset/abc" });
    await waitFor(async () => (await q.get(j, info.id)) === null);
    assert.equal(await admin.exists(`${prefix}:mail:${info.id}`), 0);
    await worker.close();
    await q.close();
  });

  await check("bullmq: get under another definition returns null", async () => {
    const a = defineJob({ name: "a", queue: "same", process: () => 1 });
    const b = defineJob({ name: "b", queue: "same", process: () => 2 });
    const q = createJobQueue({ backend: bullmqBackend({ redisUrl, prefix }), jobs: [a, b] });
    const info = await q.enqueue(a, {}, { delayMs: 60_000 });
    assert.equal((await q.get(a, info.id))?.name, "a");
    assert.equal(await q.get(b, info.id), null);
    await q.close();
  });

  await check("bullmq: a job name the worker does not know fails at once", async () => {
    const known = defineJob({ name: "known", queue: "mixed", process: () => 1 });
    const failed: { name: string; final: boolean }[] = [];
    const worker = startWorker({ redisUrl, prefix, jobs: [known], onFailed: (f) => failed.push(f) });
    const raw = new Queue("mixed", { connection: redisConnection(redisUrl), prefix });
    await raw.add("stranger", {}, { attempts: 3 });
    await waitFor(async () => failed.length > 0);
    assert.deepEqual(failed.map((f) => [f.name, f.final]), [["stranger", true]]);
    await raw.close();
    await worker.close();
  });

  await check("bullmq: a shared connection is not closed by worker-kit", async () => {
    const connection = redisConnection(redisUrl);
    const j = defineJob({ name: "shared", process: () => "x" });
    const worker = startWorker({ connection, prefix, jobs: [j] });
    const q = createJobQueue({ backend: bullmqBackend({ connection, prefix }), jobs: [j] });
    const info = await q.enqueue(j, {});
    await waitFor(async () => (await q.get(j, info.id))?.state === "completed");
    await worker.close();
    await q.close();
    assert.equal(await connection.ping(), "PONG");
    await connection.quit();
  });

  await check("bullmq + fallback: Redis down → jobs run standalone, without waiting", async () => {
    const port = await freePort(); // nothing listens here
    const down = new Redis(`redis://127.0.0.1:${port}`, { maxRetriesPerRequest: null, lazyConnect: true });
    down.on("error", quiet);
    const ran: string[] = [];
    const j = defineJob<{ to: string }, null>({ name: "mail", process: (d) => (ran.push(d.to), null) });
    const reasons: string[] = [];
    const q = createJobQueue({
      backend: fallbackBackend({ primary: bullmqBackend({ connection: down, prefix }), fallback: inlineBackend(), onFallback: (r) => reasons.push((r as Error).message) }),
      jobs: [j],
    });
    const t0 = Date.now();
    const info = await q.enqueue(j, { to: "a@example.com" });
    assert.ok(Date.now() - t0 < 1000);
    assert.equal(info.state, "completed");
    assert.deepEqual(ran, ["a@example.com"]);
    assert.match(reasons[0] ?? "", /bullmq backend is not available/);
    await q.close();
    down.disconnect();
  });

  await check("bullmq: needs a connection or a URL", async () => {
    await code(() => bullmqBackend({}), "WORKER_UNAVAILABLE");
  });

  const leftovers = await admin.keys(`${prefix}:*`);
  if (leftovers.length) await admin.del(...leftovers);
  await admin.quit();
}
redisServer?.kill();

for (const f of failures) console.log(f);
console.log(`\n${passed} passed, ${failures.length} failed`);
process.exit(failures.length ? 1 : 0);
