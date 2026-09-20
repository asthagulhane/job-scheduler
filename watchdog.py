import os
import time
import pg8000.native
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    url = urlparse(os.getenv("DATABASE_URL"))
    return pg8000.native.Connection(
        user=url.username, password=url.password, host=url.hostname,
        port=url.port or 5432, database=url.path.lstrip("/"), ssl_context=True
    )

def reclaim_expired_jobs():
    conn = get_connection()
    rows = conn.run("""
        UPDATE jobs
        SET status = 'queued', locked_by = NULL, lease_expires_at = NULL
        WHERE status = 'running' AND lease_expires_at < now()
        RETURNING id, locked_by;
    """)
    conn.close()
    for job_id, old_worker in rows:
        print(f"[watchdog] reclaimed job {job_id} (lease expired, was held by {old_worker})")

def main():
    print("[watchdog] starting, checking for expired leases every 5s...")
    while True:
        reclaim_expired_jobs()
        time.sleep(5)

if __name__ == "__main__":
    main()
    