// Jobs in this process, no Redis: development, tests, a single server, and the standalone side of
// fallbackBackend. Same definitions, same JSON data, same retries and same error messages as BullMQ.
import { backoffDelay, logError, runAttempt, type ErrorHandler } from "./define";
import { GENERIC_FAILURE, isJobFailure, WorkerError } from "./errors";
import type { AnyJob, JobBackend, JobInfo, PreparedJob } from "./types";

export interface InlineBackendOptions {
  /**
   * true (default): `add` runs the job at once and resolves when it has finished (the request waits),
   * retrying immediately. false: `add` resolves at once and the job runs in the background of this
   * process, with the definition's backoff between attempts. A delayed job always runs in the background.
   */
  wait?: boolean;
  /** Called with errors that are not a JobFailure. Default: one line on stderr. */
  onError?: ErrorHandler;
}

interface Entry {
  info: JobInfo;
  def: AnyJob;
}

export function inlineBackend(o: InlineBackendOptions = {}): JobBackend {
  const wait = o.wait ?? true;
  const onError = o.onError ?? logError;
  const jobs = new Map<string, Entry>();
  const running = new Set<Promise<void>>();
  const timers = new Map<NodeJS.Timeout, () => void>();
  let closed = false;

  const prune = () => {
    const now = Date.now();
    for (const [id, { info, def }] of jobs) {
      const finished = info.finishedAt?.getTime();
      if (finished === undefined) continue;
      const keep = info.state === "failed" ? def.keepFailedMs : def.keepResultsMs;
      if (finished + keep <= now) jobs.delete(id);
    }
  };

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      const t = setTimeout(() => {
        timers.delete(t);
        resolve();
      }, ms);
      timers.set(t, resolve);
    });

  /** Runs every attempt; never rejects. */
  async function run(entry: Entry, delayFirst: number, backoff: boolean): Promise<void> {
    const { info, def } = entry;
    if (delayFirst > 0) await sleep(delayFirst);
    else if (backoff) await new Promise((r) => setImmediate(r)); // background: after `add` has returned
    while (!closed) {
      info.state = "active";
      info.attempts++;
      try {
        info.result = await runAttempt(def, info.data, { id: info.id, name: info.name, queue: info.queue, attempt: info.attempts, attempts: def.attempts }, onError);
        info.state = "completed";
        info.error = null;
        break;
      } catch (e) {
        info.error = isJobFailure(e) ? e.message : GENERIC_FAILURE;
        if (!(isJobFailure(e) && e.retry) || info.attempts >= def.attempts) {
          info.state = "failed";
          break;
        }
        info.state = "delayed";
        if (backoff) await sleep(backoffDelay(def, info.attempts));
      }
    }
    if (info.state === "completed" || info.state === "failed") info.finishedAt = new Date();
  }

  const track = (p: Promise<void>) => {
    running.add(p);
    void p.finally(() => running.delete(p));
  };

  const snapshot = (info: JobInfo): JobInfo => ({ ...info });

  return {
    mode: "inline",
    async add(job: PreparedJob) {
      if (closed) throw new WorkerError("WORKER_CLOSED", "The job queue is closed");
      prune();
      const info: JobInfo = {
        id: job.id,
        name: job.def.name,
        queue: job.def.queue,
        state: job.delayMs > 0 ? "delayed" : "waiting",
        attempts: 0,
        error: null,
        result: null,
        data: job.data,
        createdAt: new Date(),
        finishedAt: null,
      };
      const entry: Entry = { info, def: job.def };
      jobs.set(info.id, entry);
      if (wait && job.delayMs === 0) {
        await run(entry, 0, false);
        const out = snapshot(info);
        prune(); // keepResultsMs 0: gone for `get`, but the caller still gets the outcome
        return out;
      }
      track(run(entry, job.delayMs, true));
      return snapshot(info);
    },
    async get(def, id) {
      prune();
      const e = jobs.get(id);
      return e && e.info.name === def.name && e.info.queue === def.queue ? snapshot(e.info) : null;
    },
    available: () => !closed,
    async close() {
      closed = true;
      // pending delays and backoffs end now; jobs not yet finished are dropped (nothing persists in-process)
      for (const [t, resolve] of timers) {
        clearTimeout(t);
        resolve();
      }
      timers.clear();
      await Promise.allSettled([...running]);
      jobs.clear();
    },
  };
}
