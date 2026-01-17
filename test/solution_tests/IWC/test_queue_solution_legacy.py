import pytest
from datetime import date
from solutions.IWC.queue_solution_legacy import Queue, COMPANIES_HOUSE_PROVIDER, CREDIT_CHECK_PROVIDER, BANK_STATEMENTS_PROVIDER, Priority

@pytest.fixture
def queue():
    return Queue()

def test_age(queue):
    assert queue.age == 0

def test_size_empty_queue(queue):
    assert queue.size == 0

def test_size_busy_queue(queue):
    queue.enqueue({ "provider": CREDIT_CHECK_PROVIDER, "user_id": 123, "timestamp": date(2026, 1, 15) })

    assert queue.size == 1


