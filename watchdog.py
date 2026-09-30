import os
import time
import pg8000.native
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

INTERVAL = float(os.getenv("WATCHDOG_INTERVAL", "5"))


def get_connection():
    url = urlparse(os.getenv("DATABASE_URL"))
    return pg8000.native.Connection(
        user=url.username, password=url.password, host=url.hostname,
        port=url.port or 5432, database=url.path.lstrip("/"), ssl_context=True
        if os.getenv("DB_SSL", "1") == "1" else None,
    )


def reclaim_expired_jobs(conn):
    # Counts the lost lease as a failed attempt so a job that keeps killing workers
    # ends in dead_letter instead of looping forever.
    # The CTE captures locked_by BEFORE it is nulled (RETURNING alone would show NULL).
    # SKIP LOCKED makes it safe to run several watchdogs.
    rows = conn.run("""
        WITH expired AS (
            SELECT id, locked_by FROM jobs
            WHERE status = 'running' AND lease_expires_at < now()
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs j
        SET status = CASE WHEN j.retry_count + 1 >= j.max_retries THEN 'dead_letter' ELSE 'queued' END,
            retry_count = j.retry_count + 1, locked_by = NULL, lease_expires_at = NULL
        FROM expired e
        WHERE j.id = e.id
        RETURNING j.id, e.locked_by, j.status;
    """)
    for job_id, old_worker, new_status in rows:
        print(f"[watchdog] job {job_id}: lease expired (held by {old_worker}) -> {new_status}", flush=True)


def main():
    print(f"[watchdog] starting, checking for expired leases every {INTERVAL}s...", flush=True)
    conn = None
    while True:
        try:
            if conn is None:
                conn = get_connection()
            reclaim_expired_jobs(conn)
        except Exception as e:
            print(f"[watchdog] error: {e}; reconnecting", flush=True)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()