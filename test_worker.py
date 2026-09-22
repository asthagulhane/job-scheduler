import pytest
from worker import claim_job, mark_failed, get_connection

@pytest.fixture
def conn():
    """This runs before each test, giving it a fresh DB connection.
    'yield' means: give the test this connection, then run cleanup after."""
    connection = get_connection()
    yield connection
    connection.close()

def insert_test_job(conn, payload='{"task": "test"}'):
    """Helper: inserts one test job and returns its id."""
    result = conn.run(
        "INSERT INTO jobs (payload) VALUES (:payload) RETURNING id;",
        payload=payload
    )
    return result[0][0]

def test_claim_job_returns_none_when_queue_empty(conn):
    # First, make sure nothing is queued by claiming everything that exists
    while claim_job(conn) is not None:
        pass
    # Now the queue should genuinely be empty
    result = claim_job(conn)
    assert result is None

def test_claim_job_claims_a_queued_job(conn):
    job_id = insert_test_job(conn)
    result = claim_job(conn)
    assert result is not None
    claimed_id, payload, retry_count, max_retries = result
    assert claimed_id == job_id

def test_mark_failed_requeues_under_max_retries(conn):
    job_id = insert_test_job(conn)
    claim_job(conn)  # claim it so it's 'running'
    mark_failed(conn, job_id, retry_count=0, max_retries=3)
    row = conn.run("SELECT status, retry_count FROM jobs WHERE id = :id;", id=job_id)
    status, retry_count = row[0]
    assert status == "queued"
    assert retry_count == 1

def test_mark_failed_moves_to_dead_letter_at_limit(conn):
    job_id = insert_test_job(conn)
    claim_job(conn)
    mark_failed(conn, job_id, retry_count=2, max_retries=3)  # this is the 3rd failure
    row = conn.run("SELECT status FROM jobs WHERE id = :id;", id=job_id)
    status = row[0][0]
    assert status == "dead_letter"