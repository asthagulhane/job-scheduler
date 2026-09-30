import os
import sys
import time
import random
import subprocess
import uuid
import json
from urllib.parse import urlparse
import pg8000.native
from dotenv import load_dotenv

load_dotenv()

NUM_JOBS = 20
NUM_WORKERS = 3
KILL_PHASE_SECONDS = 45
KILL_INTERVAL_RANGE = (3, 7)
SETTLE_TIMEOUT_SECONDS = 90
LOG_DIR = "chaos_logs"

def get_connection():
    url = urlparse(os.getenv("DATABASE_URL_DIRECT"))
    return pg8000.native.Connection(
        user=url.username, password=url.password, host=url.hostname,
        port=url.port or 5432, database=url.path.lstrip("/"), ssl_context=True
    )


def insert_batch(batch_id):
    conn = get_connection()
    ids = []
    for i in range(NUM_JOBS):
        payload = json.dumps({"chaos_batch": batch_id, "seq": i})
        result = conn.run(
            "INSERT INTO jobs (payload) VALUES (:payload) RETURNING id;",
            payload=payload
        )
        ids.append(result[0][0])
    conn.close()
    return ids


def get_statuses(job_ids):
    conn = get_connection()
    placeholders = ",".join(str(jid) for jid in job_ids)
    rows = conn.run(f"SELECT id, status FROM jobs WHERE id IN ({placeholders});")
    conn.close()
    return {job_id: status for job_id, status in rows}


def spawn_worker(name):
    log_path = os.path.join(LOG_DIR, f"{name}.log")
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        [sys.executable, "worker.py", name],
        stdout=log_file, stderr=subprocess.STDOUT
    )
    return proc, log_file


def spawn_watchdog():
    log_path = os.path.join(LOG_DIR, "watchdog.log")
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        [sys.executable, "watchdog.py"],
        stdout=log_file, stderr=subprocess.STDOUT
    )
    return proc, log_file


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    batch_id = str(uuid.uuid4())[:8]

    print(f"[chaos] submitting {NUM_JOBS} jobs, batch={batch_id}")
    job_ids = insert_batch(batch_id)
    print(f"[chaos] job ids: {job_ids}")

    print("[chaos] starting watchdog")
    watchdog_proc, watchdog_log = spawn_watchdog()

    workers = {}  # name -> (process, log_file)
    worker_counter = 0
    for _ in range(NUM_WORKERS):
        worker_counter += 1
        name = f"chaos-{worker_counter}"
        proc, log = spawn_worker(name)
        workers[name] = (proc, log)
        print(f"[chaos] started {name}")

    print(f"[chaos] kill phase: {KILL_PHASE_SECONDS}s of random worker kills")
    kill_count = 0
    phase_end = time.time() + KILL_PHASE_SECONDS
    while time.time() < phase_end:
        time.sleep(random.uniform(*KILL_INTERVAL_RANGE))
        if not workers:
            continue
        victim_name = random.choice(list(workers.keys()))
        proc, log = workers.pop(victim_name)
        proc.kill()
        proc.wait()
        log.close()
        kill_count += 1
        print(f"[chaos] killed {victim_name} (kill #{kill_count})")

        worker_counter += 1
        new_name = f"chaos-{worker_counter}"
        new_proc, new_log = spawn_worker(new_name)
        workers[new_name] = (new_proc, new_log)
        print(f"[chaos] replaced with {new_name}")

    print(f"[chaos] kill phase done, {kill_count} kills performed")
    print("[chaos] letting the queue settle, no more killing")

    settle_start = time.time()
    final_statuses = {}
    while time.time() - settle_start < SETTLE_TIMEOUT_SECONDS:
        statuses = get_statuses(job_ids)
        unfinished = [jid for jid, s in statuses.items() if s not in ("done", "dead_letter")]
        if not unfinished:
            final_statuses = statuses
            print(f"[chaos] all {NUM_JOBS} jobs reached a final state")
            break
        time.sleep(2)
    else:
        final_statuses = get_statuses(job_ids)
        print(f"[chaos] TIMEOUT waiting for jobs to settle")

    print("[chaos] shutting down all workers and watchdog")
    for name, (proc, log) in workers.items():
        proc.kill()
        proc.wait()
        log.close()
    watchdog_proc.kill()
    watchdog_proc.wait()
    watchdog_log.close()

    print("\n[chaos] ==== RESULTS ====")
    status_counts = {}
    for jid, status in final_statuses.items():
        status_counts[status] = status_counts.get(status, 0) + 1
    print(f"[chaos] status breakdown: {status_counts}")

    unfinished = [jid for jid in job_ids if final_statuses.get(jid) not in ("done", "dead_letter")]
    if unfinished:
        print(f"[chaos] FAIL: {len(unfinished)} jobs never reached a final state: {unfinished}")
    else:
        print(f"[chaos] PASS: all {NUM_JOBS} jobs reached done or dead_letter")

    finished_counts = {}
    for name in list(workers.keys()) + [f"chaos-{i}" for i in range(1, worker_counter + 1)]:
        log_path = os.path.join(LOG_DIR, f"{name}.log")
        if not os.path.exists(log_path):
            continue
        with open(log_path) as f:
            for line in f:
                if "finished job" in line:
                    try:
                        jid = int(line.split("finished job")[1].strip().split()[0])
                        finished_counts[jid] = finished_counts.get(jid, 0) + 1
                    except (IndexError, ValueError):
                        pass

    double_finished = {jid: count for jid, count in finished_counts.items() if count > 1 and jid in job_ids}
    if double_finished:
        print(f"[chaos] FAIL: these jobs were finished more than once: {double_finished}")
    else:
        print(f"[chaos] PASS: no job in this batch was finished more than once")

    print(f"[chaos] {kill_count} workers killed mid-run during the test")
    print(f"[chaos] logs saved in ./{LOG_DIR}/")


if __name__ == "__main__":
    main()