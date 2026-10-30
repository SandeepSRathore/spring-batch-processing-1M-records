# How this project was built

A record of how the transaction import was designed, built and verified: the decisions taken,
what the Spring Batch 6 source code turned out to require, the bugs that only live failure
testing exposed, and what is still unverified. The [README](../README.md) covers how to run it;
the original design is in [specs/](superpowers/specs/2026-10-30-transaction-import-design.md).

## 1. Requirements

The brief: a Spring Boot project that uses Spring Batch to process 1M records from a CSV (or any
input format), taking care of everything that can go wrong during processing.

Clarified before building:

| Question | Answer |
|---|---|
| Learning exercise or production-like? | Designed like production, records saved to a database. |
| Real data or generated? | Generated customer transactions, including deliberately bad rows. |
| What does "process" mean? | Validate, clean (trim, upper-case codes, convert types) and save. Nothing heavier (no fraud scoring, no external calls). |

Choices made for the user (stated up front, open to override): Java 25 and Maven (already
installed), Postgres in Docker, a REST trigger instead of run-on-startup, refusing a file that was
already imported, failing the job beyond a configurable number of bad rows, and a
single-threaded step so restarts stay exact.

## 2. Design

The core idea everything else rests on: **each chunk commits its inserted rows, its rejection
records and Spring Batch's saved reader position in one database transaction.** Whatever the
database kept after any failure is therefore self-consistent, and a restart resumes from exactly
that point. Inserts are also idempotent (`ON CONFLICT (transaction_id) DO NOTHING`) as a second
line of defence.

```
CSV ──► FlatFileItemReader ──► TransactionValidatingProcessor ──► TransactionWriter ──► Postgres
        (strings + line no.)     (all rule violations per row)     (batch insert,          │
              │                          │                          duplicates → rejections)
              └── parse error ──┐        └── validation error ──┐                         │
                                ▼                               ▼                         │
                        RejectionSkipListener ──► rejected_transaction ◄──────────────────┘
                                   (same transaction as the chunk)
```

Around the job: `ImportService` and the REST API (start, status, rejections, stop, restart),
and `ImportJobLifecycle` (automatic restart on transient failures, recovery of runs a crashed
process left behind, graceful shutdown).

## 3. Reading the framework before trusting it

Spring Boot 4.1.2 ships Spring Batch 6.0.6, which replaced the chunk-processing engine
(`ChunkOrientedStep`), moved most packages and switched retry to Spring Framework 7. Rather than
rely on memory or older docs, the jars' class layout and the 6.0.6 **source code** were read
before writing the step. That changed the design in three places:

1. **No retry inside the step.** Batch 6 retries a failed write *inside the same transaction*.
   On Postgres, a transaction is aborted after any error, so an in-transaction retry cannot
   succeed. Instead, a run that fails on a transient error is **restarted from its last
   checkpoint** with exponential backoff (`TransientFailures` decides what counts as transient:
   connection loss, deadlock, serialization failure, server shutdown, etc.).
2. **No skips on write.** A skippable write error makes Batch roll the chunk back and re-scan it
   item by item. That rollback would also discard the rejection rows already recorded for read
   and process skips in the same chunk, silently losing them. So the writer never throws for bad
   data: it reads the per-row JDBC update counts and records rows that hit an existing
   `transaction_id` as `DUPLICATE` rejections itself.
3. **Reader defaults that lose data.** `FlatFileItemReader` by default treats lines starting
   with `#` as comments and drops them, and its default record-separator policy lets one stray
   quote swallow every following line up to the end of the file into a single "record". Both
   were turned off: one physical line is one record, and every line is accounted for.

Also learned from the source: skip-listener callbacks run inside the chunk transaction (so
recording rejections there is safe), `JobOperator.recover()` exists for crashed runs, and a
full worker pool makes the launcher mark the new run `FAILED` with a `TaskRejectedException`
(mapped to HTTP 503).

## 4. Building it

Order of work: schema (Flyway manages both the app tables and the Spring Batch metadata tables),
domain types, processor, reader, writer, skip listener, job and step wiring, lifecycle, service,
REST API. Then the test-data generator and the tests.

Issues hit while wiring it up:

| Problem | Fix |
|---|---|
| Bean cycle at startup: Boot's batch configuration resolves the task executor in its constructor; the executor's decorator needed the lifecycle, which needed the `JobRepository` being created. | The executor looks the decorator up lazily (`ObjectProvider`). |
| Spring Batch saves a job's final status *before* calling listeners. If the database is down at that moment, `afterJob` never runs and the lifecycle never learns the job ended. | A `TaskDecorator` on the job executor catches a job thread that ends without `afterJob` and schedules a restart. *(Not yet exercised; see §7.)* |
| The stored exit description was a full stack trace, with the real cause cut off by truncation. | `afterJob` stores a short cause chain instead (e.g. `FatalStepExecutionException <- SkipLimitExceededException ...`). |
| `getJobExecution` throws instead of returning null for an unknown ID. | Mapped to 404. |

## 5. Testing

- **Unit:** `TransactionValidatingProcessorTest`, 20 cases covering every validation rule.
- **Integration:** `TransactionImportIntegrationTest`, 14 tests against real Postgres via
  Testcontainers. A test-only Postgres trigger injects failures on chosen rows, with a chosen
  SQLState. A "one-shot" fault uses a sequence, because sequence increments survive a rollback.
  That makes it possible to test:
  - every kind of bad line in one fixture, with exact line numbers and reasons;
  - a failure in the 4th chunk, then a restart: no duplicates, no lost or doubled rejections;
  - a transient fault restarted automatically, a permanent one not, and giving up after the
    retry limit;
  - stop then resume, the skip limit, a file changed before restart, and a run left `STARTED`
    by a crashed process;
  - the REST API's status codes and responses.

The suite was run repeatedly to rule out flakiness. Two timing problems were found and fixed:
a test emptied the tables while an earlier test's job was still finishing, and a status read
landed in the gap between Spring Batch's two final saves.

**A real bug the tests found: the restart position could move backwards after a stop.**
`JobOperator.stop()` saves a copy of the step execution it loaded moments earlier, and the step's
execution context is written *without a version check*. If the step commits a chunk in between,
the saved reader position goes back, and a restart re-reads committed lines. The duplicate guard
stopped bad data getting in, but 9 lines were processed twice. Fix: stop by flagging only the
job execution as `STOPPING`. The running step checks that at every commit, and job-level updates
reconcile version numbers. The stop/resume test now asserts zero rejections after resuming.

## 6. Live verification

Unit and integration tests simulate failures. These runs used the real application, a real
1M-row file and real failures.

| Scenario | Result |
|---|---|
| 1M lines with ~1% bad, `kill -9` at line 100,000, restart the app, re-submit the file | Startup marked the orphaned run `FAILED`; re-submitting resumed at line 100,001. **990,036 inserted, 615 duplicates, 2,492 parse errors, 6,857 validation errors**: exactly what the generator predicted. 1,000,000 lines accounted for, no duplicate IDs. ~15k rows/s. |
| Re-submit the completed file / ask for `../pom.xml` | 409 / 400. |
| **Outage #1:** Postgres stopped mid-import | **Bug:** the rollback failed along with the connection, so Batch marked the run `UNKNOWN`, which it refuses to restart, and the import sat there. The data was consistent (exactly 64 committed chunks), confirming the transaction design. Fixed: such runs are marked `FAILED` before restarting. |
| **Outage #2:** same test after the fix | **Bug:** different timing. The database was down when the step saved its status (step still `STARTED`) but back for the job's (`FAILED`). Batch won't restart while a step is recorded as running, and the recovery only repaired the job-level record. All 3 automatic restarts were refused. Fixed: `markRestartable` repairs job **and** step records left running or `UNKNOWN`, on every restart path. Added an integration test recreating this state. |
| Stuck runs from #1 and #2, restarted by hand after the fixes | Both resumed at their last commit (lines 64,001 and 67,001); exact expected totals. |
| **Outage #3:** fresh file, Postgres stopped ~4 s in (line ~68,000) for 15 s | Postgres healthy at 23:14:53; the first automatic restart repaired the step record and resumed at 23:14:57; **completed at 23:15:10 with no human involved**. 297,014 inserted, 169 / 763 / 2,054 rejected, exactly as predicted. 297,014 rows, 297,014 distinct IDs. |

**Lesson:** two of the three recovery bugs were only visible with a real outage, where the
order in which the step and job statuses get (or fail to get) saved depends on exact timing.
Paths that have not been run against real failures should be treated as unproven.

## 7. Known gaps

Never exercised, either by a test or live:
- the 503 when the concurrency limit is reached;
- graceful shutdown (SIGTERM) *during* an import (only tested on an idle app);
- the safety net for a job that ends without saving its final status (§4).

Missing for real production use:
- **No authentication.** Anyone who can reach port 8080 can start, stop or restart imports and
  read rejected rows.
- **Invalid UTF-8 bytes** are silently replaced with `�` rather than rejecting the row, so
  corrupted text can be imported.
- **Auto-restart window** is 3 attempts over ~35 s by default. A longer outage needs a manual
  restart (nothing is lost). Configurable.
- **Single instance per database**: startup recovery treats every run it does not own as
  crashed. Several instances need a lease or lock first.
- The skip limit applies per run, not across restarts of the same file.
- Rejections are kept forever (no retention).
- No bulk-load tuning: pgjdbc's `reWriteBatchedInserts` would be faster, but hides the
  per-row update counts that duplicate detection relies on.
