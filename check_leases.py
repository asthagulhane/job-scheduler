from benchmark import get_connection

conn = get_connection()

rows = conn.run("""
    SELECT
        id,
        status,
        locked_by,
        lease_expires_at,
        now() AS db_now,
        lease_expires_at < now() AS expired
    FROM jobs
    WHERE status = 'running'
    ORDER BY id;
""")

print("\n=== RUNNING JOB LEASES ===")

for row in rows:
    print(row)

conn.close()