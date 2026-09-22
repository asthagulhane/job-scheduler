# Job Scheduler

A distributed job queue backed directly by PostgreSQL — no external message broker (no Redis, no RabbitMQ, no SQS). The database *is* the queue.

## Why this exists

This project is deliberately not another "FastAPI + SQL" wrapper. It's a systems-engineering project: the core challenge is concurrency control and crash recovery, not any particular framework.

| | This project |
|---|---|
| Uses an LLM? | No — zero AI |
| Role of FastAPI | Thin producer-side API only (accepts job submissions) |
| Role of SQL | The queue itself — write-heavy, transactional, must handle real race conditions |
| Core challenge | Multiple worker processes safely claiming jobs with zero double-processing, and recovering automatically when a worker crashes |

## The locking strategy (the actual point of this project)

Every worker runs this, inside a single transaction:

    BEGIN;

    SELECT id, payload FROM jobs
    WHERE status = 'queued'
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    UPDATE jobs
    SET status = 'running', locked_by = 'worker-1', lease_expires_at = now() + interval '15 seconds'
    WHERE id = <claimed id>;

    COMMIT;

Why FOR UPDATE alone isn't enough: if two workers run this SELECT at the same instant on the same row, Postgres won't let them both claim it — but without SKIP LOCKED, the second worker just blocks and waits for the first transaction to commit. That's safe, but with many workers polling constantly, you get a pile-up of workers all queued behind whoever locked a row first.

Why SKIP LOCKED fixes this: instead of blocking, a worker that finds a row already locked simply skips it and checks the next one immediately. Workers fan out across available jobs with essentially zero contention.

## Crash recovery

This is why locked_by and lease_expires_at are two separate columns instead of a single is_locked boolean:

- A boolean can only say whether something is locked — not for how long it's allowed to stay locked. If the worker holding a boolean lock crashes, the lock stays true forever.
- lease_expires_at turns the lock into a time-boxed promise: "I claim this job, but if I don't renew by this timestamp, treat me as dead."
- A separate watchdog.py process checks every 5 seconds for any job where status = 'running' AND lease_expires_at < now(), and resets it back to queued.

Workers extend their own lease via periodic heartbeats while actively processing a job.

## Retry logic

Each job tracks retry_count against max_retries; on failure, a job is either requeued (if under the limit) or moved to a permanent dead_letter status (if not).

## Tech stack

- Python + FastAPI — producer API only
- PostgreSQL (hosted on Neon) — chosen over SQLite because SQLite has no row-level locking
- pg8000 — pure-Python Postgres driver

## Verified results

Tested with 3 concurrent worker processes against a live Postgres instance:
- 13 jobs processed, zero jobs ever claimed by two workers
- Retry-with-backoff self-corrected 2 simulated failures without manual intervention
- Watchdog correctly reclaimed real jobs left stuck by crashed workers during development

## Running it

Install: pip install fastapi uvicorn pg8000 python-dotenv sqlalchemy

Set DATABASE_URL in a .env file, then run main.py with uvicorn, worker.py (one or more), and watchdog.py.

Live demo: https://taskforge-astha-hrfvgjh2avfxhkd7.centralindia-01.azurewebsites.net/docs
