import os
import sys
import time
import random
import pg8000.native
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()
worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker-unknown"

def get_connection():
    url = urlparse(os.getenv("DATABASE_URL_DIRECT"))
    return pg8000.native.Connection(
        user=url.username, password=url.password, host=url.hostname,
        port=url.port or 5432, database=url.path.lstrip("/"), ssl_context=True
    )

def claim_job(conn):
    conn.run("BEGIN;")
    rows = conn.run("""
        SELECT id, payload, retry_count, max_retries FROM jobs
        WHERE status = 'queued'
        ORDER BY priority ASC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    """)
    if not rows:
        conn.run("COMMIT;")
        return None
    job_id, payload, retry_count, max_retries = rows[0]
    conn.run(
        "UPDATE jobs SET status='running', locked_by=:w, lease_expires_at=now() + interval '15 seconds' WHERE id=:id;",
        w=worker_id, id=job_id
    )
    conn.run("COMMIT;")
    return job_id, payload, retry_count, max_retries

def heartbeat(conn, job_id):
    result = conn.run(
        "UPDATE jobs SET lease_expires_at = now() + interval '15 seconds' WHERE id = :id AND locked_by = :worker RETURNING id;",
        id=job_id, worker=worker_id
    )
    if not result:
        raise RuntimeError(f"Lost lease on job {job_id} — another worker may own it now")

def mark_done(conn, job_id):
    result = conn.run(
        "UPDATE jobs SET status = 'done' WHERE id = :id AND locked_by = :worker RETURNING id;",
        id=job_id, worker=worker_id
    )
    if not result:
        print(f"[{worker_id}] lost lease on job {job_id} before finishing — not marking done")

def mark_failed(conn, job_id, retry_count, max_retries):
    if retry_count + 1 >= max_retries:
        result = conn.run(
            "UPDATE jobs SET status = 'dead_letter', retry_count = retry_count + 1 WHERE id = :id AND locked_by = :worker RETURNING id;",
            id=job_id, worker=worker_id
        )
        if result:
            print(f"[{worker_id}] job {job_id} moved to dead_letter after {retry_count + 1} attempts")
        else:
            print(f"[{worker_id}] lost lease on job {job_id} — not touching it")
    else:
        result = conn.run(
            "UPDATE jobs SET status = 'queued', retry_count = retry_count + 1, locked_by = NULL, lease_expires_at = NULL WHERE id = :id AND locked_by = :worker RETURNING id;",
            id=job_id, worker=worker_id
        )
        if result:
            print(f"[{worker_id}] job {job_id} failed, requeued (attempt {retry_count + 1})")
        else:
            print(f"[{worker_id}] lost lease on job {job_id} — not touching it")

def do_work(conn, job_id):
    # simulated work: heartbeat every 5s across a 12s task, ~20% simulated failure chance
    for _ in range(2):
        time.sleep(5)
        heartbeat(conn, job_id)
    time.sleep(2)
    if random.random() < 0.2:
        raise RuntimeError("simulated task failure")

def main():
    print(f"[{worker_id}] starting, polling for jobs...")
    while True:
        try:
            conn = get_connection()
            job = claim_job(conn)
        except Exception as e:
            print(f"[{worker_id}] connection error, retrying: {e}")
            time.sleep(2)
            continue

        if job:
            job_id, payload, retry_count, max_retries = job
            print(f"[{worker_id}] claimed job {job_id}: {payload}")
            try:
                do_work(conn, job_id)
                mark_done(conn, job_id)
                print(f"[{worker_id}] finished job {job_id}")
            except Exception as e:
                print(f"[{worker_id}] job {job_id} raised error: {e}")
                mark_failed(conn, job_id, retry_count, max_retries)
            finally:
                conn.close()
        else:
            conn.close()
            time.sleep(1)

if __name__ == "__main__":
    main()
