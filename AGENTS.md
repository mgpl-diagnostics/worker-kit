# AGENTS.md — @mgpl-diagnostics/worker-kit

Background jobs for the MGPL Diagnostics backend: typed job definitions, one queue API over three
backends (BullMQ on Redis, in-process, worker-or-standalone fallback) and the worker runner.
**A library: no server, no GraphQL, no business logic.** The api repo defines the jobs and wires
them (container + `src/worker.ts`); mail-kit queues through `enqueuer()`.

## Map

```
src/types.ts      JobDefinition, JobInfo, JobState, JobContext, JobBackend, PreparedJob
src/errors.ts     WorkerError (codes), JobFailure (safe message + retry flag), isJobFailure
src/define.ts     defineJob, SECRET_KEY, toJsonData / prepareJob (checks in the caller), runAttempt, backoffDelay
src/queue.ts      createJobQueue (enqueue, get, enqueuer, close), jobRegistry
src/inline.ts     inlineBackend (wait or background, retries, backoff, retention)
src/fallback.ts   fallbackBackend (primary when available, else fallback)
src/bullmq.ts     "/bullmq" entry: bullmqBackend, startWorker, redisConnection, redisReady, bullJobOptions
scripts/test.ts   definitions, data rules, inline, fallback, enqueuer; the same scenario on inline and BullMQ (real Redis)
```

## Rules

1. **Every backend behaves the same.** Data and results cross JSON everywhere; retries, backoff,
   states, attempt counts and error messages match. Change behaviour in one backend → change it in
   all, and keep the shared scenario in `scripts/test.ts` passing on inline *and* BullMQ.
2. **Failures are safe to show.** Only a `JobFailure`'s message reaches `JobInfo.error`; every other
   error goes to `onError` and reads "Job failed". All attempts go through `runAttempt`.
3. **Payloads carry ids, never secrets.** `SECRET_KEY` keys are refused in the caller; do not loosen it
   by default. Jobs whose data must not linger use `keepResultsMs: 0`.
4. **Checks happen in the caller** (`prepareJob`): bad data fails the request, not the worker.
5. **Zero runtime dependencies in the main entry.** Only `src/bullmq.ts` imports `bullmq`/`ioredis`
   (optional peers); CI greps `dist/index.js` for it.
6. Job ids are random UUIDs; `get` returns a job only under its own definition. Ownership checks
   belong to the caller (the api's services).
7. Error codes (`WorkerError.code`) and state names (`JobState`) are public: add, never rename.
8. Backends are structural (`JobBackend`); prefer that over new options when adding a queue technology.
9. Never commit secrets, `.env` files or patient data. Tests use fictional values.

## Commands

```bash
npm run typecheck && npm test && npm run build
npm run llms      # regenerate llms-full.txt (CI fails when it is stale)
```

`npm test` needs Redis for the BullMQ part: `REDIS_URL`, or a local `redis-server` (started on a free
port). CI runs a Redis service with `REQUIRE_REDIS=1`.

## Repository rules

`main` takes pull requests only: squash merge, 1 code-owner review (`@BackStacked`), the `ci` check
required. Rules come from `mgpl-diagnostics/.github` (`scripts/protect-repos.mjs`).

## Licence

All rights reserved (see LICENSE).
