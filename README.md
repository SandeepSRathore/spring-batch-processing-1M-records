# Transaction Import (Spring Boot 4 + Spring Batch 6)

Imports a CSV of ~1M customer transactions into Postgres, built to survive what goes wrong in a
real import: bad rows, crashes mid-file, database outages, re-runs and operator stops.

Measured on a laptop: **1,000,000 lines in ~65 s (~15k rows/s)**, including a `kill -9` at
line 100,000 and a resume that ended with every line accounted for (990,036 inserted, 9,964
rejected, exactly as generated).

## Run it

```bash
docker compose up -d --wait                                  # Postgres 17 on :5432
java scripts/GenerateTransactions.java 1000000 data/transactions.csv   # prints the expected outcome
mvn spring-boot:run                                          # or: mvn package && java -jar target/*.jar

curl -X POST localhost:8080/api/imports -H 'Content-Type: application/json' -d '{"file":"transactions.csv"}'
curl localhost:8080/api/imports/1                            # status + totals
curl 'localhost:8080/api/imports/1/rejections?limit=20'      # what was rejected and why
```

Tests (Docker required, uses Testcontainers): `mvn test` (34 tests: 20 unit, 14 integration
against real Postgres with injected database faults).

Generator options: `java scripts/GenerateTransactions.java [rows] [output] [badRatio] [seed] [firstId]`.
Use a different `firstId` to create a file whose transaction ids do not overlap an earlier one.

## API

| Call | Purpose |
|---|---|
| `POST /api/imports {"file": "name.csv"}` | Start (or resume) an import. `202` + `Location`. File must be inside `app.import.input-dir`. |
| `GET /api/imports/{id}` | Status of a run, plus totals across all runs of that file. |
| `GET /api/imports/{id}/rejections?offset=&limit=` | Rejected lines with line number, raw text and reason. |
| `POST /api/imports/{id}/stop` | Stop after the current chunk; ends `STOPPED`, restartable. |
| `POST /api/imports/{id}/restart` | Resume a `FAILED`/`STOPPED`/`UNKNOWN` run from its last committed chunk. |

Errors are RFC 9457 problem details: `400` bad file/request, `404` unknown run, `409` already
imported / already running / not restartable, `503` too many concurrent imports.

## What can go wrong, and what happens

| Situation | Behaviour |
|---|---|
| Malformed line (wrong column count, blank, stray quote, `#...`) | Skipped, stored in `rejected_transaction` as `PARSE_ERROR` with line number and raw text. A stray quote cannot swallow the following lines (one physical line = one record). |
| Valid CSV but bad data (amount, currency, date, enum, ids, future date...) | Skipped as `VALIDATION_ERROR`; the reason lists *every* rule the row breaks. |
| Duplicate `transaction_id` (in the file, or already in the DB) | Not inserted (`ON CONFLICT DO NOTHING`), recorded as `DUPLICATE`. |
| File is broken as a whole (missing, empty, wrong header, outside input dir) | Refused up front with `400`, before a job is created. |
| Too many bad rows (`app.import.skip-limit`, default 50,000 per run) | Job fails: the file is probably wrong as a whole. |
| Same file submitted again after success | `409`. Job identity = path + SHA-256 of the content. |
| Same file submitted after a failure/stop | Resumes that import from its last checkpoint. |
| File replaced between a failure and the restart | Restart refused (checksum mismatch) instead of resuming at line N of a different file. |
| Process killed (`kill -9`, OOM, host reboot) | Run is left `STARTED`; at next startup it is marked `FAILED`, then resume it via restart/re-submit. Nothing half-written: rows, rejections and reader position commit in one transaction. |
| Transient DB error (connection lost, deadlock, serialization failure, DB restarting) | Run fails cleanly and is **restarted automatically** from its last checkpoint with exponential backoff (`app.import.auto-restart.*`). Permanent errors are not retried. |
| DB dies mid-run (rollback fails too, or status saved only partly) | Depending on timing Spring Batch leaves the run `UNKNOWN`, or the job `FAILED` with its step still `STARTED`, and refuses to restart either. Because data and checkpoint share one transaction, resuming is safe: such records are marked `FAILED` before any restart (see `ImportJobLifecycle#markRestartable`), and the automatic restart then resumes the import. Verified live by stopping Postgres mid-import. |
| Database unreachable when the job finishes | Detected by a task decorator on the job executor (listeners are skipped in that case); a restart is scheduled. |
| Graceful shutdown (SIGTERM) | Running jobs are asked to stop at the next chunk boundary, end `STOPPED`, and can be resumed. |
| Too many imports at once (`app.import.max-concurrent-jobs`) | `503` instead of queueing silently. |
| NUL bytes / huge lines in a corrupt file | Sanitised and truncated before being stored as rejections. |

## Design notes

- **One transaction per chunk** covers the inserted rows, their rejection records and Spring
  Batch's saved reader position. This is the property everything else relies on: whatever the
  database committed is consistent, so a restart never re-inserts or loses lines.
- **No skips on write.** A skippable write error makes Spring Batch roll back and re-scan the
  chunk item by item, which would also roll back rejections already recorded for that chunk.
  Duplicates are detected from JDBC update counts instead, keeping the fast path.
- **No in-step retry.** Spring Batch 6 retries writes inside the same transaction, which is
  useless on Postgres (the transaction is aborted after an error). The unit of retry is a
  restart from the last checkpoint instead.
- **Stopping uses a job-level flag, not `JobOperator.stop()`.** The latter also saves a stale copy
  of the step execution whose execution context is written without a version check; if the step
  commits at the same moment, the saved reader position moves backwards and a restart re-reads
  committed lines. (Found by the stop/resume test.)
- **Single-threaded step on purpose.** `FlatFileItemReader` is not thread-safe and its restart
  position only means something for one sequential reader. If you need more throughput, the
  next step is partitioning the file by line ranges (one reader per partition), not a
  multi-threaded step.
- Schema (app tables *and* Spring Batch metadata) is managed by Flyway.

## Known gaps and limits

- **No authentication** on the API: add it before exposing the service anywhere.
- Invalid UTF-8 bytes are replaced with `�` instead of rejecting the row.
- Not yet exercised by tests or live runs: the 503 at the concurrency limit, SIGTERM during a
  running import, and the safety net for a job that ends without saving its final status.
- One application instance per database: startup recovery treats every `STARTED` run it does
  not own as crashed. Running several instances needs a lease/lock (e.g. ShedLock) first.
- Automatic restarts: 3 attempts over ~35 s by default; longer outages need a manual restart.
- The skip limit applies per run, not cumulatively across restarts.
- Rejections are kept forever; add retention if files are imported continuously.

How this was designed, built and verified, including the bugs that live failure testing
found: [docs/DEVELOPMENT_PROCESS.md](docs/DEVELOPMENT_PROCESS.md).
