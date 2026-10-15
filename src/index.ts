// @mgpl-diagnostics/worker-kit: background jobs. The main entry has no dependencies; BullMQ lives in "/bullmq".
export { backoffDelay, defineJob, logError, prepareJob, runAttempt, SECRET_KEY, toJsonData, type DefineJobOptions, type ErrorHandler } from "./define";
export { GENERIC_FAILURE, isJobFailure, JobFailure, WorkerError, type WorkerErrorCode } from "./errors";
export { fallbackBackend, type FallbackBackendOptions } from "./fallback";
export { inlineBackend, type InlineBackendOptions } from "./inline";
export { createJobQueue, jobRegistry, type Enqueuer, type JobQueue, type JobQueueOptions } from "./queue";
export type { AnyJob, DataOf, EnqueueOptions, JobBackend, JobContext, JobDefinition, JobInfo, JobState, PreparedJob, ResultOf } from "./types";
