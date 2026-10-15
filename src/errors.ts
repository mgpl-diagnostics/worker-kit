/** Error codes are a public contract: add, never rename. */
export type WorkerErrorCode =
  | "WORKER_UNKNOWN_JOB" // enqueue/get/run of a job name that is not defined
  | "WORKER_BAD_DATA" // data is not JSON, too large, carries a secret key, or failed validate()
  | "WORKER_DUPLICATE_JOB" // two definitions with the same name
  | "WORKER_UNAVAILABLE" // no backend could take the job
  | "WORKER_CLOSED"; // the queue was closed

export class WorkerError extends Error {
  constructor(readonly code: WorkerErrorCode, message: string) {
    super(message);
    this.name = "WorkerError";
  }
}

/**
 * Throw from a processor for a failure whose message is safe to show to the client.
 * `retry: false` fails the job at once (retrying cannot help); `true` uses the remaining attempts.
 * Any other error is logged through `onError` and shown as "Job failed".
 */
export class JobFailure extends Error {
  constructor(message: string, readonly retry: boolean = true) {
    super(message);
    this.name = "JobFailure";
  }
}

/** True for a JobFailure, also one made by another copy of this package. */
export function isJobFailure(e: unknown): e is JobFailure {
  return e instanceof JobFailure || (e instanceof Error && e.name === "JobFailure" && typeof (e as { retry?: unknown }).retry === "boolean");
}

/** The message shown for failures that are not a JobFailure. */
export const GENERIC_FAILURE = "Job failed";
