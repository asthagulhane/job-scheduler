@'
# Job Scheduler

A distributed job queue backed directly by PostgreSQL - no external message broker (no Redis, no RabbitMQ, no SQS). The database *is* the queue.

## Why this exists

This project is deliberately not another "FastAPI + SQL" wrapper. It's a systems-engineering project: the core challenge is concurrency control and crash recovery, not any particular framework.

| | This project |
|---|---|
| Uses an LLM? | No - zero AI |
| Role of FastAPI | Thin producer-side API only (accepts job submissions) |
| Role of SQL | The queue itself - write-heavy, transactional, must handle real race conditions |
| Core challenge | Multiple worker processes safely claiming jobs with zero double-processing, and recovering automatically when a worker crashes |

## The locking strategy (the actual point of this project)

Every worker runs this, inside a single transaction:
