// The producer API: one object the app's services use to start jobs and read them back,
// whatever backend sits underneath.
import { prepareJob } from "./define";
import { WorkerError } from "./errors";
import type { AnyJob, DataOf, EnqueueOptions, JobBackend, JobInfo, ResultOf } from "./types";

export interface JobQueueOptions {
  backend: JobBackend;
  /** Every job this queue may start. Names are unique. */
  jobs: readonly AnyJob[];
}

/** A one-job producer with mail-kit's `MailQueue` shape: `createMailer({ queue: jobs.enqueuer(mailJob) })`. */
export interface Enqueuer<Data> {
  /** Starts the job; resolves with its id. Rejects when it could not be queued or (in-process) failed. */
  enqueue(data: Data): Promise<string>;
  available(): Promise<boolean>;
}

export interface JobQueue {
  readonly mode: string;
  /** Validates the data here (bad data fails the caller), then hands the job to the backend. */
  enqueue<J extends AnyJob>(job: J, data: DataOf<J>, o?: EnqueueOptions): Promise<JobInfo<DataOf<J>, ResultOf<J>>>;
  enqueue(job: string, data: unknown, o?: EnqueueOptions): Promise<JobInfo>;
  /** The job with this id, only if it is a `job` job. Callers still check that the reader owns it. */
  get<J extends AnyJob>(job: J, id: string): Promise<JobInfo<DataOf<J>, ResultOf<J>> | null>;
  get(job: string, id: string): Promise<JobInfo | null>;
  enqueuer<J extends AnyJob>(job: J): Enqueuer<DataOf<J>>;
  definition(name: string): AnyJob;
  names(): string[];
  available(): Promise<boolean>;
  close(): Promise<void>;
}

/** Checks a set of definitions: unique names. Returns them by name. */
export function jobRegistry(jobs: readonly AnyJob[]): Map<string, AnyJob> {
  const byName = new Map<string, AnyJob>();
  for (const j of jobs) {
    if (byName.has(j.name)) throw new WorkerError("WORKER_DUPLICATE_JOB", `Two jobs are named "${j.name}"`);
    byName.set(j.name, j);
  }
  return byName;
}

export function createJobQueue(o: JobQueueOptions): JobQueue {
  const byName = jobRegistry(o.jobs);
  let closed = false;

  const resolve = (job: AnyJob | string): AnyJob => {
    const name = typeof job === "string" ? job : job.name;
    const def = byName.get(name);
    if (!def) throw new WorkerError("WORKER_UNKNOWN_JOB", `Unknown job "${name}" (known: ${[...byName.keys()].join(", ") || "none"})`);
    if (typeof job !== "string" && job !== def) throw new WorkerError("WORKER_UNKNOWN_JOB", `Job "${name}" is not the definition this queue was created with`);
    return def;
  };

  const enqueue = async (job: AnyJob | string, data: unknown, opts?: EnqueueOptions): Promise<JobInfo> => {
    if (closed) throw new WorkerError("WORKER_CLOSED", "The job queue is closed");
    const def = resolve(job);
    return o.backend.add(prepareJob(def, data, opts));
  };

  const available = async () => {
    if (closed) return false;
    try {
      return (await (o.backend.available?.() ?? true)) === true;
    } catch {
      return false;
    }
  };

  const queue: JobQueue = {
    mode: o.backend.mode,
    enqueue: enqueue as JobQueue["enqueue"],
    get: (async (job: AnyJob | string, id: string) => (typeof id === "string" && id ? o.backend.get(resolve(job), id) : null)) as JobQueue["get"],
    enqueuer(job) {
      const def = resolve(job);
      return {
        async enqueue(data) {
          const info = await enqueue(def, data);
          if (info.state === "failed") throw new WorkerError("WORKER_UNAVAILABLE", `Job "${def.name}" failed: ${info.error ?? "Job failed"}`);
          return info.id;
        },
        available,
      };
    },
    definition: (name) => resolve(name),
    names: () => [...byName.keys()],
    available,
    async close() {
      if (closed) return;
      closed = true;
      await o.backend.close();
    },
  };
  return queue;
}
