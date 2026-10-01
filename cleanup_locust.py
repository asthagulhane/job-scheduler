from benchmark import get_connection

conn = get_connection()

rows = conn.run("""
    DELETE FROM jobs
    WHERE status = 'queued'
      AND payload->>'source' = 'locust'
    RETURNING id;
""")

print(f"Deleted {len(rows)} queued Locust test jobs.")

conn.close()