import os
import time
import json
import logging
import pg8000.native
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("taskforge")

app = FastAPI()

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

class JobRequest(BaseModel):
    payload: dict

@app.post("/jobs")
def create_job(job: JobRequest):
    last_error = None
    for attempt in range(3):
        try:
            conn = get_connection()
            result = conn.run(
                "INSERT INTO jobs (payload) VALUES (:payload) RETURNING id, status;",
                payload=json.dumps(job.payload)
            )
            conn.close()
            job_id, status = result[0]
            logger.info(f"Job {job_id} created with status={status}")
            return {"id": job_id, "status": status}
        except Exception as e:
            last_error = e
            logger.warning(f"create_job attempt {attempt+1} failed: {e}")
            time.sleep(1)
    logger.error(f"create_job failed after 3 attempts: {last_error}")
    raise HTTPException(status_code=503, detail=f"Database unavailable after retries: {last_error}")
@app.get("/health")
def health():
    try:
        conn = get_connection()
        conn.run("SELECT 1;")
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Database unreachable: {e}")

@app.get("/stats")
def stats():
    conn = get_connection()
    rows = conn.run("SELECT status, COUNT(*) FROM jobs GROUP BY status;")
    conn.close()
    counts = {status: count for status, count in rows}
    return {
        "queued": counts.get("queued", 0),
        "running": counts.get("running", 0),
        "done": counts.get("done", 0),
        "dead_letter": counts.get("dead_letter", 0),
        "total": sum(counts.values())
    }