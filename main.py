import os
import time
import json
import pg8000.native
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

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
            return {"id": job_id, "status": status}
        except Exception as e:
            last_error = e
            time.sleep(1)
    raise HTTPException(status_code=503, detail=f"Database unavailable after retries: {last_error}")