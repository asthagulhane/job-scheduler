import os
import sys
import time
import pg8000.native
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker-unknown"

def get_connection():
    url = urlparse(os.getenv("DATABASE_URL"))
    return pg8000.native.Connection(
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip("/"),
        ssl_context=True
    )

def claim_job(conn):
    conn.run("BEGIN;")
    rows = conn.run("""
        SELECT id, payload FROM jobs
        WHERE status = 'queued'
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    """)
    if not rows:
        conn.run("COMMIT;")
        return None

    job_id, payload = rows[0]
    conn.run(
        "UPDATE jobs SET status = 'running', locked_by = :worker, lease_expires_at = now() + interval '30 seconds' WHERE id = :id;",
        worker=worker_id, id=job_id
    )
    conn.run("COMMIT;")
    return job_id, payload

def main():
    print(f"[{worker_id}] starting, polling for jobs...")
    while True:
        conn = get_connection()
        job = claim_job(conn)
        conn.close()

        if job:
            job_id, payload = job
            print(f"[{worker_id}] claimed job {job_id}: {payload}")
            time.sleep(3)
            print(f"[{worker_id}] finished job {job_id}")
        else:
            time.sleep(1)

if __name__ == "__main__":
    main()
