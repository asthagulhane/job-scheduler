import os
import pg8000.native
from urllib.parse import urlparse
from fastapi import FastAPI
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
    conn = get_connection()
    result = conn.run(
        "INSERT INTO jobs (payload) VALUES (:payload) RETURNING id, status;",
        payload=job.payload
    )
    conn.close()
    job_id, status = result[0]
    return {"id": job_id, "status": status}