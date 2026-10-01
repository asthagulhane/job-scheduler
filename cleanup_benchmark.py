from benchmark import get_connection

conn = get_connection()

rows = conn.run("""
    DELETE FROM jobs
    WHERE payload ? 'benchmark_run'
    RETURNING id;
""")

print(f"Deleted {len(rows)} failed benchmark jobs.")

conn.close()