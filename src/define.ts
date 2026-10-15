import { randomUUID } from "node:crypto";
import { GENERIC_FAILURE, isJobFailure, JobFailure, WorkerError } from "./errors";
import type { AnyJob, EnqueueOptions, JobContext, JobDefinition, PreparedJob } from "./types";

export interface DefineJobOptions<Data, Result> {
  name: string;
  queue?: string;
  validate?: (data: Data) => void;
  process: (data: Data, ctx: JobContext) => Promise<Result> | Result;
  /** Default 3. */
  attempts?: number;
  /** First retry delay; doubles each time. Default 2 s. */
  backoffMs?: number;
  /** Default 1 hour. Use 0 for jobs whose data must not linger (e.g. a mail with a reset link). */
  keepResultsMs?: number;
  /** Default max(1 day, keepResultsMs). */
  keepFailedMs?: number;
  /** Default 512 KiB. */
  maxBytes?: number;
  /** Default true. */
  refuseSecrets?: boolean;
}

const NAME = /^[a-z0-9][a-z0-9._-]{0,63}$/;

/** Keys that must not appear in job data: payloads carry ids, never tokens or secrets. */
export const SECRET_KEY = /^(password|passwordhash|token|accesstoken|refreshtoken|resettoken|secret|clientsecret|apikey|api_key|authorization|cookie)$/i;

export function defineJob<Data, Result>(o: DefineJobOptions<Data, Result>): JobDefinition<Data, Result> {
  const queue = o.queue ?? o.name;
  if (!NAME.test(o.name)) throw new WorkerError("WORKER_BAD_DATA", `Job name "${o.name}" must match ${NAME}`);
  if (!NAME.test(queue)) throw new WorkerError("WORKER_BAD_DATA", `Queue name "${queue}" must match ${NAME}`);
  const attempts = Math.max(1, Math.floor(o.attempts ?? 3));
  const keepResultsMs = Math.max(0, o.keepResultsMs ?? 3600_000);
  return Object.freeze({
    name: o.name,
    queue,
    validate: o.validate,
    process: o.process,
    attempts,
    backoffMs: Math.max(0, o.backoffMs ?? 2000),
    keepResultsMs,
    keepFailedMs: Math.max(0, o.keepFailedMs ?? Math.max(86_400_000, keepResultsMs)),
    maxBytes: o.maxBytes ?? 512 * 1024,
    refuseSecrets: o.refuseSecrets ?? true,
  });
}

/** Delay before attempt `n + 1` after `n` failed attempts (exponential, the same as BullMQ's). */
export const backoffDelay = (def: AnyJob, failedAttempts: number) => def.backoffMs * 2 ** (failedAttempts - 1);

function findSecret(v: unknown, path: string, seen = 0): string | null {
  if (seen > 64 || v === null || typeof v !== "object") return null;
  if (Array.isArray(v)) {
    for (let i = 0; i < v.length; i++) {
      const hit = findSecret(v[i], `${path}[${i}]`, seen + 1);
      if (hit) return hit;
    }
    return null;
  }
  for (const [k, child] of Object.entries(v)) {
    if (SECRET_KEY.test(k)) return path ? `${path}.${k}` : k;
    const hit = findSecret(child, path ? `${path}.${k}` : k, seen + 1);
    if (hit) return hit;
  }
  return null;
}

/** JSON round trip with the definition's checks, so every backend sees exactly what Redis would hold. */
export function toJsonData(def: AnyJob, data: unknown): unknown {
  let json: string | undefined;
  try {
    json = JSON.stringify(data);
  } catch (e) {
    throw new WorkerError("WORKER_BAD_DATA", `Job "${def.name}": data is not JSON (${(e as Error).message})`);
  }
  if (json === undefined) throw new WorkerError("WORKER_BAD_DATA", `Job "${def.name}": data is not JSON`);
  const bytes = Buffer.byteLength(json);
  if (bytes > def.maxBytes) throw new WorkerError("WORKER_BAD_DATA", `Job "${def.name}": data is ${bytes} bytes, the limit is ${def.maxBytes}`);
  const parsed: unknown = JSON.parse(json);
  if (def.refuseSecrets) {
    const key = findSecret(parsed, "");
    if (key) throw new WorkerError("WORKER_BAD_DATA", `Job "${def.name}": data carries a secret-looking key "${key}"; pass an id and look the value up in the processor`);
  }
  return parsed;
}

/** Validates and serialises a job for a backend. Runs in the caller, so bad data fails the request. */
export function prepareJob(def: AnyJob, data: unknown, o: EnqueueOptions = {}): PreparedJob {
  if (def.validate) {
    try {
      def.validate(data);
    } catch (e) {
      throw e instanceof WorkerError ? e : new WorkerError("WORKER_BAD_DATA", `Job "${def.name}": ${(e as Error).message}`);
    }
  }
  return {
    id: randomUUID(), // unguessable; callers still check ownership on every read
    def,
    data: toJsonData(def, data),
    delayMs: Math.max(0, Math.floor(o.delayMs ?? 0)),
  };
}

export type ErrorHandler = (error: unknown, ctx: JobContext) => void;

/**
 * Runs one attempt. Every backend goes through here, so failures read the same everywhere:
 * a JobFailure keeps its message and retry flag; anything else goes to `onError` (log it) and
 * becomes a retryable "Job failed", so internals never reach the client.
 */
export async function runAttempt(def: AnyJob, data: unknown, ctx: JobContext, onError?: ErrorHandler): Promise<unknown> {
  let result: unknown;
  try {
    result = await def.process(data, ctx);
  } catch (e) {
    if (isJobFailure(e)) throw e;
    onError?.(e, ctx);
    throw new JobFailure(GENERIC_FAILURE, true);
  }
  if (result === undefined) return null;
  try {
    return JSON.parse(JSON.stringify(result)) as unknown;
  } catch (e) {
    onError?.(e, ctx);
    throw new JobFailure(GENERIC_FAILURE, false);
  }
}

/** The default onError: one line on stderr, without the job's data. */
export const logError: ErrorHandler = (e, ctx) => {
  console.error(`[worker-kit] ${ctx.queue}/${ctx.name} job ${ctx.id} attempt ${ctx.attempt}/${ctx.attempts} failed:`, e);
};
