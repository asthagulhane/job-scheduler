import os
import sys
import time
import json
import subprocess
from urllib.parse import urlparse

import pg8000.native
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    database_url = os.getenv("DATABASE_URL_DIRECT")

    if not database_url:
        raise RuntimeError("DATABASE_URL_DIRECT is not set.")

    url = urlparse(database_url)

    return pg8000.native.Connection(
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip("/"),
        ssl_context=True,
    )


def queued_count(conn):
    rows = conn.run("""
        SELECT COUNT(*)
        FROM jobs
        WHERE status = 'queued';
    """)
    return rows[0][0]


def insert_jobs(conn, count, run_id):
    print(f"Creating {count} benchmark jobs...")

    for i in range(count):
        payload = json.dumps({
            "benchmark_run": run_id,
            "job_number": i,
        })

        conn.run("""
            INSERT INTO jobs (payload, status)
            VALUES (:payload, 'queued');
        """, payload=payload)

    print("Benchmark jobs created.")


def completed_count(conn, run_id):
    rows = conn.run("""
        SELECT COUNT(*)
        FROM jobs
        WHERE status = 'done'
          AND payload::text LIKE :pattern;
    """, pattern=f"%{run_id}%")

    return rows[0][0]


def remaining_count(conn, run_id):
    rows = conn.run("""
        SELECT COUNT(*)
        FROM jobs
        WHERE status = 'queued'
          AND payload::text LIKE :pattern;
    """, pattern=f"%{run_id}%")

    return rows[0][0]


def run_workers(strategy, workers, run_id, job_count):
    print()
    print("=" * 60)
    print(f"STRATEGY: {strategy}")
    print(f"WORKERS:  {workers}")
    print(f"JOBS:     {job_count}")
    print("=" * 60)

    processes = []
    log_files = []

    for i in range(workers):
        worker_id = f"bench-{strategy}-{i + 1}"
        log_name = f"bench_{strategy}_{i + 1}.log"

        log_file = open(
            log_name,
            "w",
            encoding="utf-8"
        )

        process = subprocess.Popen(
            [
                sys.executable,
                "bench_worker.py",
                worker_id,
                strategy,
                run_id,
            ],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )

        processes.append(process)
        log_files.append(log_file)

    start = time.time()

    print()
    print("Workers started.")
    print("Waiting for all benchmark jobs to complete...")

    last_done = -1

    while True:
        time.sleep(1)

        try:
            conn = get_connection()

            done = completed_count(
                conn,
                run_id
            )

            remaining = remaining_count(
                conn,
                run_id
            )

            conn.close()

        except Exception as e:
            print(f"Database check error: {e}")
            continue

        if done != last_done:
            print(
                f"Progress: {done}/{job_count} completed | "
                f"{remaining} queued"
            )
            last_done = done

        if done >= job_count:
            print()
            print("All benchmark jobs completed.")
            break

        elapsed = time.time() - start

        if elapsed >= 120:
            print()
            print("WARNING: Benchmark exceeded 120 seconds.")
            print(f"Completed: {done}/{job_count}")
            print(f"Remaining: {remaining}")
            break

    elapsed = time.time() - start

    print()
    print("Stopping workers...")

    for process in processes:
        if process.poll() is None:
            process.terminate()

    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()

    for log_file in log_files:
        log_file.close()

    print(f"Workers ran for {elapsed:.2f} seconds.")

    return elapsed


def read_logs(strategy, workers):
    processed_jobs = []

    for i in range(workers):
        filename = f"bench_{strategy}_{i + 1}.log"

        if not os.path.exists(filename):
            continue

        with open(
            filename,
            "r",
            encoding="utf-8"
        ) as f:

            for line in f:
                if "processed job" in line:
                    try:
                        job_id = (
                            line
                            .strip()
                            .split("processed job")[1]
                            .strip()
                        )

                        processed_jobs.append(job_id)

                    except Exception:
                        pass

    return processed_jobs


def analyze(strategy, workers):
    processed = read_logs(
        strategy,
        workers
    )

    unique_jobs = set(processed)

    duplicates = (
        len(processed)
        - len(unique_jobs)
    )

    print()
    print(f"Results for {strategy}")
    print("-" * 40)
    print(f"Total claims:      {len(processed)}")
    print(f"Unique jobs:       {len(unique_jobs)}")
    print(f"Duplicate claims:  {duplicates}")

    if duplicates > 0:
        print("WARNING: Duplicate processing detected.")
    else:
        print("SUCCESS: No duplicate processing detected.")

    return (
        len(processed),
        len(unique_jobs),
        duplicates,
    )


def run_benchmark(
    strategy,
    workers=3,
    job_count=100
):
    run_id = (
        f"BENCHMARK_{strategy}_{int(time.time())}"
    )

    conn = get_connection()

    try:
        existing = queued_count(conn)

        print()
        print(f"Currently queued jobs: {existing}")

        if existing != 0:
            print()
            print(
                "STOP: There are already queued jobs "
                "in the database."
            )
            print("Clean/process them first.")
            print()
            return

        insert_jobs(
            conn,
            job_count,
            run_id
        )

        print(
            f"Inserted benchmark jobs: "
            f"{job_count}"
        )

    finally:
        conn.close()

    elapsed = run_workers(
        strategy=strategy,
        workers=workers,
        run_id=run_id,
        job_count=job_count,
    )

    processed, unique, duplicates = analyze(
        strategy,
        workers
    )

    conn = get_connection()

    try:
        done = completed_count(
            conn,
            run_id
        )

        remaining = remaining_count(
            conn,
            run_id
        )

    finally:
        conn.close()

    print()
    print("=" * 60)
    print("FINAL BENCHMARK RESULTS")
    print("=" * 60)
    print(f"Strategy:                 {strategy}")
    print(f"Workers:                  {workers}")
    print(f"Jobs inserted:            {job_count}")
    print(f"Worker log claims:        {processed}")
    print(f"Unique jobs processed:    {unique}")
    print(f"Duplicate claims:         {duplicates}")
    print(f"Benchmark jobs completed: {done}")
    print(f"Benchmark jobs remaining: {remaining}")
    print(f"Elapsed time:             {elapsed:.2f}s")
    print("=" * 60)

    if (
        duplicates == 0
        and done == job_count
        and remaining == 0
    ):
        print(
            "SUCCESS: All benchmark jobs completed "
            "with no duplicate processing."
        )
    elif duplicates > 0:
        print("WARNING: Duplicate processing detected.")
    else:
        print("WARNING: Benchmark did not complete all jobs.")

    print("=" * 60)


def main():
    if len(sys.argv) > 1:
        strategy = sys.argv[1]

        if strategy not in {
            "naive",
            "for_update",
            "skip_locked",
        }:
            print(
                "Usage: python benchmark.py "
                "[naive|for_update|skip_locked]"
            )
            return

    else:
        print()
        print("Job Scheduler Concurrency Benchmark")
        print("=" * 60)
        print()
        print("1. naive")
        print("2. for_update")
        print("3. skip_locked")
        print()

        choice = input(
            "Enter 1, 2, or 3: "
        ).strip()

        strategies = {
            "1": "naive",
            "2": "for_update",
            "3": "skip_locked",
        }

        strategy = strategies.get(choice)

        if not strategy:
            print("Invalid choice.")
            return

    run_benchmark(
        strategy=strategy,
        workers=3,
        job_count=100,
    )


if __name__ == "__main__":
    main()