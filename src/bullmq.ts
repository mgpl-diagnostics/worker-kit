// "/bullmq" entry: the BullMQ-on-Redis backend (producer) and the worker runner (consumer).
// bullmq and ioredis are optional peers; nothing else in the package imports them.
import { Job, Queue, UnrecoverableError, Worker, type ConnectionOptions, type JobsOptions } from "bullmq";
import { Redis } from "ioredis";
import { logError, runAttempt, type ErrorHandler } from "./define";
import { isJobFailure, WorkerError } from "./errors";
import { jobRegistry } from "./queue";
import type { AnyJob, JobBackend, JobInfo, JobState, PreparedJob } from "./types";

export interface RedisOptions {
  /** An ioredis connection you own (not closed by worker-kit). Give this or `redisUrl`. */
  connection?: Redis;
  /** worker-kit opens (and closes) its own connection. */
  redisUrl?: string;
  /** Redis key prefix: keys are `<prefix>:<queue>:…`. Default "mgpl". */
  prefix?: string;
}

/** An ioredis connection as BullMQ needs it (`maxRetriesPerRequest: null` for blocking commands). */
export function redisConnection(url: string): Redis {
  return new Redis(url, { maxRetriesPerRequest: null });
}

/** `available` check for a connection: true only while it is "ready" (ioredis would otherwise queue commands while down). */
export const redisReady = (connection: Redis) => () => connection.status === "ready";

function own(o: RedisOptions): { connection: Redis; owned: boolean } {
  if (o.connection) return { connection: o.connection, owned: false };
  if (o.redisUrl) {
    const connection = redisConnection(o.redisUrl);
    // ioredis reconnects by itself; `available()` reports the outage. Without a listener every retry is printed.
    connection.on("error", () => undefined);
    return { connection, owned: true };
  }
  throw new WorkerError("WORKER_UNAVAILABLE", "Give a Redis `connection` or a `redisUrl`");
}

const seconds = (ms: number) => Math.max(1, Math.ceil(ms / 1000));

/** BullMQ job options from a definition: the only place they are set. */
export function bullJobOptions(job: PreparedJob): JobsOptions {
  const { def } = job;
  return {
    jobId: job.id,
    attempts: def.attempts,
    backoff: { type: "exponential", delay: def.backoffMs },
    ...(job.delayMs > 0 ? { delay: job.delayMs } : {}),
    removeOnComplete: def.keepResultsMs === 0 ? true : { age: seconds(def.keepResultsMs) },
    removeOnFail: def.keepFailedMs === 0 ? true : { age: seconds(def.keepFailedMs) },
  };
}

const STATES: Record<string, JobState> = { waiting: "waiting", "waiting-children": "waiting", prioritized: "waiting", active: "active", completed: "completed", failed: "failed", delayed: "delayed" };

async function toInfo(job: Job, queue: string): Promise<JobInfo> {
  const state = STATES[await job.getState()] ?? "waiting";
  return {
    id: job.id!,
    name: job.name,
    queue,
    state,
    attempts: job.attemptsMade,
    error: state === "completed" ? null : job.failedReason || null,
    result: state === "completed" ? ((job.returnvalue as unknown) ?? null) : null,
    data: job.data as unknown,
    createdAt: new Date(job.timestamp),
    finishedAt: job.finishedOn && (state === "completed" || state === "failed") ? new Date(job.finishedOn) : null,
  };
}

/**
 * The producer side (API processes): adds jobs to Redis. Run `startWorker` somewhere to process them.
 * `available()` is false while the Redis connection is not "ready" (use it with fallbackBackend).
 */
export function bullmqBackend(o: RedisOptions): JobBackend {
  const { connection, owned } = own(o);
  const prefix = o.prefix ?? "mgpl";
  const queues = new Map<string, Queue>();
  const queueFor = (name: string) => {
    let q = queues.get(name);
    if (!q) {
      q = new Queue(name, { connection: connection as unknown as ConnectionOptions, prefix });
      queues.set(name, q);
    }
    return q;
  };
  return {
    mode: "bullmq",
    async add(job) {
      const added = await queueFor(job.def.queue).add(job.def.name, job.data, bullJobOptions(job));
      return toInfo(added, job.def.queue);
    },
    async get(def: AnyJob, id: string) {
      const job = await Job.fromId(queueFor(def.queue), id);
      return job && job.name === def.name ? toInfo(job, def.queue) : null;
    },
    available: redisReady(connection),
    async close() {
      await Promise.all([...queues.values()].map((q) => q.close()));
      if (owned) await connection.quit().catch(() => undefined);
    },
  };
}

export interface StartWorkerOptions extends RedisOptions {
  /** The definitions to run. One BullMQ Worker per distinct queue. */
  jobs: readonly AnyJob[];
  /** Only these queues (default: every queue of `jobs`). */
  queues?: readonly string[];
  /** Jobs a worker runs at once, per queue. Default 4. */
  concurrency?: number;
  /** Errors that are not a JobFailure (with the job context). Default: one line on stderr. */
  onError?: ErrorHandler;
  /** Each failed attempt (after onError). Default: one warning line. */
  onFailed?: (info: { id: string | undefined; name: string; queue: string; attempt: number; error: string; final: boolean }) => void;
}

export interface RunningWorker {
  queues: string[];
  workers: Worker[];
  /** Finishes the jobs in hand, then stops. */
  close(): Promise<void>;
}

/** The consumer side (`node dist/worker.js` in the api): runs the definitions' processors. */
export function startWorker(o: StartWorkerOptions): RunningWorker {
  const byName = jobRegistry(o.jobs);
  const { connection, owned } = own(o);
  const prefix = o.prefix ?? "mgpl";
  const onError = o.onError ?? logError;
  const onFailed =
    o.onFailed ?? ((f) => console.warn(`[worker-kit] ${f.queue}/${f.name} job ${f.id} failed (attempt ${f.attempt}${f.final ? ", final" : ""}): ${f.error}`));
  const names = [...new Set(o.jobs.map((j) => j.queue))].filter((q) => !o.queues || o.queues.includes(q));

  const workers = names.map((queue) => {
    const w = new Worker(
      queue,
      async (job: Job) => {
        const def = byName.get(job.name);
        if (!def || def.queue !== queue) throw new UnrecoverableError(`Unknown job "${job.name}" on queue "${queue}"`);
        const ctx = { id: job.id ?? "", name: job.name, queue, attempt: job.attemptsMade + 1, attempts: job.opts.attempts ?? def.attempts };
        try {
          return await runAttempt(def, job.data, ctx, onError);
        } catch (e) {
          const message = isJobFailure(e) ? e.message : "Job failed";
          // a JobFailure that retrying cannot fix fails the job at once
          if (isJobFailure(e) && !e.retry) throw new UnrecoverableError(message);
          throw new Error(message);
        }
      },
      { connection: connection as unknown as ConnectionOptions, prefix, concurrency: Math.max(1, o.concurrency ?? 4) },
    );
    w.on("failed", (job, err) => {
      if (!job) return;
      const final = err instanceof UnrecoverableError || err.name === "UnrecoverableError" || job.attemptsMade >= (job.opts.attempts ?? 1);
      onFailed({ id: job.id, name: job.name, queue, attempt: job.attemptsMade, error: err.message, final });
    });
    // without a listener an "error" event (e.g. Redis dropped) would crash the process
    w.on("error", (err) => console.error(`[worker-kit] ${queue} worker:`, err.message));
    return w;
  });

  return {
    queues: names,
    workers,
    async close() {
      await Promise.all(workers.map((w) => w.close()));
      if (owned) await connection.quit().catch(() => undefined);
    },
  };
}
