# TaskForge — PostgreSQL Distributed Job Scheduler

A distributed job queue backed directly by PostgreSQL — no external message broker (no Redis, no RabbitMQ, no SQS). The database *is* the queue.

## Why this exists

This project is deliberately not another "FastAPI + SQL" wrapper. It is a systems-engineering project focused on concurrency control, crash recovery, retries, and reliable job processing.

| | This project |
|---|---|
| Uses an LLM? | No — zero AI |
| Role of FastAPI | Thin producer-side API for job submission and job inspection |
| Role of PostgreSQL | The queue itself — transactional, concurrent, and durable |
| Core challenge | Multiple worker processes safely claiming jobs without double-processing |
| Recovery mechanism | Time-boxed leases + watchdog |
| Reliability features | Retries, backoff, dead-letter queue, idempotency keys |

## Architecture

```text
                ┌─────────────────────┐
                │     FastAPI API     │
                │   POST /jobs        │
                │   GET /jobs/{id}    │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │     PostgreSQL      │
                │                     │
                │     jobs table      │
                │    "the queue"      │
                └───────┬─────┬───────┘
                        │     │
              ┌─────────┘     └─────────┐
              ▼                         ▼
       ┌─────────────┐           ┌─────────────┐
       │   Worker 1  │           │   Worker 2  │
       └─────────────┘           └─────────────┘
              │                         │
              └────────────┬────────────┘
                           ▼
                    ┌─────────────┐
                    │  Watchdog   │
                    │ lease expiry│
                    └─────────────┘