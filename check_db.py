from benchmark import get_connection

conn = get_connection()

print("\n=== JOB STATUS ===")

rows = conn.run("""
    SELECT status, COUNT(*)
    FROM jobs
    GROUP BY status
    ORDER BY status;
""")

for row in rows:
    print(row)


print("\n=== QUEUED JOB COUNT ===")

rows = conn.run("""
    SELECT COUNT(*)
    FROM jobs
    WHERE status = 'queued';
""")

print("Queued:", rows[0][0])


print("\n=== RUNNING JOBS ===")

rows = conn.run("""
    SELECT
        id,
        status,
        locked_by,
        lease_expires_at,
        retry_count
    FROM jobs
    WHERE status = 'running'
    ORDER BY id;
""")

if rows:
    for row in rows:
        print(row)
else:
    print("No running jobs.")


print("\n=== FIRST 20 QUEUED JOBS ===")

rows = conn.run("""
    SELECT
        id,
        status,
        payload,
        retry_count
    FROM jobs
    WHERE status = 'queued'
    ORDER BY id
    LIMIT 20;
""")

if rows:
    for row in rows:
        print(row)
else:
    print("No queued jobs.")


conn.close()