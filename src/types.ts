// The job model shared by every backend. A job is JSON data on a named queue; a definition says
// how to run it. Backends (in-process, BullMQ on Redis, fallback) only move and store jobs.

/** A job's state, the same words on every backend. */
export type JobState = "waiting" | "delayed" | "active" | "completed" | "failed";

/** What a processor gets besides the data. */
export interface JobContext {
  id: string;
  name: string;
  queue: string;
  /** 1 on the first run, 2 on the first retry, … */
  attempt: number;
  /** Total attempts allowed. */
  attempts: number;
}

/**
 * A kind of background work. Data and result cross JSON (Dates arrive as ISO strings), on every
 * backend, so a job behaves the same in-process and on Redis.
 */
export interface JobDefinition<Data = unknown, Result = unknown> {
  /** Job name, unique in a queue set, e.g. "password-reset". */
  readonly name: string;
  /** Queue it runs on (Redis keys `<prefix>:<queue>:…`). Default: the name. */
  readonly queue: string;
  /** Runs before enqueueing, in the caller's request: throw (WorkerError WORKER_BAD_DATA) to refuse. */
  readonly validate?: ((data: Data) => void) | undefined;
  readonly process: (data: Data, ctx: JobContext) => Promise<Result> | Result;
  /** Total attempts, with exponential backoff between them. */
  readonly attempts: number;
  readonly backoffMs: number;
  /** How long a finished job (and its result) stays readable. 0 = removed at once. */
  readonly keepResultsMs: number;
  /** How long a failed job stays readable (at least a day by default, for inspection). */
  readonly keepFailedMs: number;
  /** Largest JSON payload accepted, in bytes. */
  readonly maxBytes: number;
  /** Refuse data with token / password / secret keys (see SECRET_KEY). */
  readonly refuseSecrets: boolean;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyJob = JobDefinition<any, any>;
export type DataOf<J> = J extends JobDefinition<infer D, unknown> ? D : never;
export type ResultOf<J> = J extends JobDefinition<unknown, infer R> ? R : never;

/** A job as callers see it. */
export interface JobInfo<Data = unknown, Result = unknown> {
  id: string;
  name: string;
  queue: string;
  state: JobState;
  /** Attempts made so far. */
  attempts: number;
  /** Safe to show: a JobFailure's message, otherwise a generic "Job failed". */
  error: string | null;
  result: Result | null;
  data: Data;
  createdAt: Date;
  finishedAt: Date | null;
}

export interface EnqueueOptions {
  /** Run no sooner than this many ms from now. */
  delayMs?: number;
}

/** A job ready for a backend: JSON-checked data plus the backend-independent options. */
export interface PreparedJob {
  id: string;
  def: AnyJob;
  data: unknown;
  delayMs: number;
}

/**
 * Where jobs go. Implementations: `inlineBackend` (this process), `bullmqBackend` ("/bullmq",
 * Redis) and `fallbackBackend` (a worker when available, else standalone). Write your own the same way.
 */
export interface JobBackend {
  /** "inline", "bullmq", "fallback" or your own; shown in logs and health checks. */
  readonly mode: string;
  add(job: PreparedJob): Promise<JobInfo>;
  /** The job with this id on `def`'s queue, or null (unknown, expired, or another job's name). */
  get(def: AnyJob, id: string): Promise<JobInfo | null>;
  /** Cheap readiness check (e.g. the Redis connection is "ready"). Default: assume yes. */
  available?(): boolean | Promise<boolean>;
  close(): Promise<void>;
}
