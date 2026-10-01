import os
import sys
import time
import pg8000.native
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

worker_id = sys.argv[1]
strategy = sys.argv[2]
run_id = sys.argv[3]


def get_connection():
    url = urlparse(os.getenv("DATABASE_URL_DIRECT"))

    return pg8000.native.Connection(
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip("/"),
        ssl_context=True
    )


def claim_naive(conn):
    rows = conn.run("""
        SELECT id, payload
        FROM jobs
        WHERE status = 'queued'
          AND payload::text LIKE :pattern
        ORDER BY created_at
        LIMIT 1;
    """, pattern=f"%{run_id}%")

    if not rows:
        return None

    job_id, payload = rows[0]

    conn.run("""
        UPDATE jobs
        SET status = 'running',
            locked_by = :w
        WHERE id = :id;
    """, w=worker_id, id=job_id)

    return job_id, payload


def claim_for_update(conn):
    conn.run("BEGIN;")

    rows = conn.run("""
        SELECT id, payload
        FROM jobs
        WHERE status = 'queued'
          AND payload::text LIKE :pattern
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE;
    """, pattern=f"%{run_id}%")

    if not rows:
        conn.run("COMMIT;")
        return None

    job_id, payload = rows[0]

    conn.run("""
        UPDATE jobs
        SET status = 'running',
            locked_by = :w
        WHERE id = :id;
    """, w=worker_id, id=job_id)

    conn.run("COMMIT;")

    return job_id, payload


def claim_skip_locked(conn):
    conn.run("BEGIN;")

    rows = conn.run("""
        SELECT id, payload
        FROM jobs
        WHERE status = 'queued'
          AND payload::text LIKE :pattern
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    """, pattern=f"%{run_id}%")

    if not rows:
        conn.run("COMMIT;")
        return None

    job_id, payload = rows[0]

    conn.run("""
        UPDATE jobs
        SET status = 'running',
            locked_by = :w
        WHERE id = :id;
    """, w=worker_id, id=job_id)

    conn.run("COMMIT;")

    return job_id, payload


CLAIM_FUNCS = {
    "naive": claim_naive,
    "for_update": claim_for_update,
    "skip_locked": claim_skip_locked,
}


def main():
    claim_fn = CLAIM_FUNCS[strategy]

    conn = get_connection()

    try:
        while True:
            try:
                job = claim_fn(conn)

                if job:
                    job_id, payload = job

                    # Simulate a small amount of work.
                    time.sleep(0.02)

                    conn.run("""
                        UPDATE jobs
                        SET status = 'done'
                        WHERE id = :id;
                    """, id=job_id)

                    print(
                        f"[{worker_id}] processed job {job_id}",
                        flush=True
                    )

                else:
                    time.sleep(0.1)

            except Exception as e:
                print(
                    f"[{worker_id}] ERROR: {e}",
                    flush=True
                )
                time.sleep(0.2)

    finally:
        conn.close()


if __name__ == "__main__":
    main()