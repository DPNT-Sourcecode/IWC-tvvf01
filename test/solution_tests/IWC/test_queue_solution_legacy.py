import pytest
from datetime import date
from solutions.IWC.queue_solution_legacy import Queue, COMPANIES_HOUSE_PROVIDER, CREDIT_CHECK_PROVIDER, BANK_STATEMENTS_PROVIDER, Priority
from solutions.IWC.task_types import TaskSubmission


@pytest.fixture
def queue():
    return Queue()


def test_age(queue):
    assert queue.age == 0


def test_size_empty_queue(queue):
    assert queue.size == 0


def test_size_busy_queue(queue):
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER, user_id=123, timestamp=date(2026, 1, 15))
    bank_statement_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER, user_id=123, timestamp=date(2026, 1, 15))
    queue.enqueue(credit_check_task)
    queue.enqueue(bank_statement_task)
    queue.dequeue()

    assert queue.size == 1


def test_purge(queue):
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER, user_id=123, timestamp=date(2026, 1, 15))
    bank_statement_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER, user_id=123, timestamp=date(2026, 1, 15))
    purge_result = queue.purge()

    assert purge_result == True
    assert queue.size == 0


# def 



