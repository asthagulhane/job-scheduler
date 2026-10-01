from benchmark import get_connection

conn = get_connection()

rows = conn.run("""
    SELECT
        payload->>'source' AS source,
        COUNT(*)
    FROM jobs
    WHERE status = 'queued'
    GROUP BY payload->>'source'
    ORDER BY source;
""")

print("\n=== QUEUED JOB SOURCES ===")

for row in rows:
    print(row)

conn.close()