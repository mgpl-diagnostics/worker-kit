// "The worker if it is there, otherwise standalone": jobs go to the primary backend (BullMQ on
// Redis) while it is available, and run on the fallback (usually in-process) when it is not.
import { WorkerError } from "./errors";
import type { JobBackend, PreparedJob } from "./types";

export interface FallbackBackendOptions {
  primary: JobBackend;
  fallback: JobBackend;
  /** When the primary is unavailable or `add` fails: "fallback" (default) or "throw" (WORKER_UNAVAILABLE). */
  whenFails?: "fallback" | "throw";
  /** Called each time a job went to the fallback (log it). */
  onFallback?: (reason: unknown, job: PreparedJob) => void;
}

export function fallbackBackend(o: FallbackBackendOptions): JobBackend {
  const { primary, fallback } = o;
  const isAvailable = async (b: JobBackend) => {
    try {
      return (await (b.available?.() ?? true)) === true;
    } catch {
      return false;
    }
  };
  return {
    mode: "fallback",
    async add(job) {
      let reason: unknown;
      if (await isAvailable(primary)) {
        try {
          return await primary.add(job);
        } catch (e) {
          reason = e;
        }
      } else {
        reason = new WorkerError("WORKER_UNAVAILABLE", `The ${primary.mode} backend is not available`);
      }
      if (o.whenFails === "throw") {
        throw reason instanceof WorkerError ? reason : new WorkerError("WORKER_UNAVAILABLE", `Could not queue job "${job.def.name}": ${reason instanceof Error ? reason.message : String(reason)}`);
      }
      o.onFallback?.(reason, job);
      return fallback.add(job);
    },
    async get(def, id) {
      // ids are UUIDs, so a job is on one side only; a primary that is down just has nothing to say
      const fromPrimary = (await isAvailable(primary)) ? await primary.get(def, id).catch(() => null) : null;
      return fromPrimary ?? fallback.get(def, id);
    },
    async available() {
      return (await isAvailable(primary)) || (await isAvailable(fallback));
    },
    async close() {
      await Promise.all([primary.close(), fallback.close()]);
    },
  };
}
