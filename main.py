import os
import time
import json
import logging
import queue
import pg8000.native
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("taskforge")

app = FastAPI()

POOL_SIZE = 5
connection_pool = queue.Queue(maxsize=POOL_SIZE)

def create_connection():
    url = urlparse(os.getenv("DATABASE_URL"))
    return pg8000.native.Connection(
        user=url.username, password=url.password, host=url.hostname,
        port=url.port or 5432, database=url.path.lstrip("/"), ssl_context=True
    )

@app.on_event("startup")
def startup():
    for _ in range(POOL_SIZE):
        connection_pool.put(create_connection())
    logger.info(f"Connection pool initialized with {POOL_SIZE} connections")

def get_pooled_connection():
    return connection_pool.get()

def return_connection(conn):
    connection_pool.put(conn)

class JobRequest(BaseModel):
    payload: dict

@app.post("/jobs")
def create_job(job: JobRequest):
    conn = get_pooled_connection()
    try:
        result = conn.run(
            "INSERT INTO jobs (payload) VALUES (:payload) RETURNING id, status;",
            payload=json.dumps(job.payload)
        )
        job_id, status = result[0]
        logger.info(f"Job {job_id} created with status={status}")
        return {"id": job_id, "status": status}
    except Exception as e:
        logger.error(f"create_job failed: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {e}")
    finally:
        return_connection(conn)

@app.get("/health")
def health():
    conn = get_pooled_connection()
    try:
        conn.run("SELECT 1;")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {e}")
    finally:
        return_connection(conn)

@app.get("/stats")
def stats():
    conn = get_pooled_connection()
    try:
        rows = conn.run("SELECT status, COUNT(*) FROM jobs GROUP BY status;")
        counts = {status: count for status, count in rows}
        return {
            "queued": counts.get("queued", 0),
            "running": counts.get("running", 0),
            "done": counts.get("done", 0),
            "dead_letter": counts.get("dead_letter", 0),
            "total": sum(counts.values())
        }
    finally:
        return_connection(conn)