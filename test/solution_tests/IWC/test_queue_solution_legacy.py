import pytest
from datetime import date, datetime
from solutions.IWC.queue_solution_legacy import Queue, COMPANIES_HOUSE_PROVIDER, CREDIT_CHECK_PROVIDER, BANK_STATEMENTS_PROVIDER, ID_VERIFICATION_PROVIDER, Priority
from solutions.IWC.task_types import TaskSubmission


datetime1 = datetime(2026, 1, 17, 19, 30)
datetime2 = datetime(2026, 1, 17, 18, 30)


@pytest.fixture
def queue():
    return Queue()


def test_size_empty_queue(queue):
    assert queue.size == 0


def test_size_busy_queue(queue):
    companies_house_task = TaskSubmission(provider=COMPANIES_HOUSE_PROVIDER.name, user_id=123, timestamp=datetime2)
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1)
    bank_statement_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER.name, user_id=123, timestamp=datetime1)
    queue.enqueue(companies_house_task)
    assert queue.enqueue(credit_check_task) == 2
    queue.enqueue(bank_statement_task)

    assert queue.dequeue().provider == COMPANIES_HOUSE_PROVIDER.name
    assert queue.dequeue().provider == CREDIT_CHECK_PROVIDER.name
    assert queue.dequeue().provider == BANK_STATEMENTS_PROVIDER.name
    assert queue.size == 0


def test_purge(queue):
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1)
    purge_result = queue.purge()

    assert purge_result == True
    assert queue.size == 0


def test_enqueue_respects_dependency_resolution(queue):
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1)

    assert queue.enqueue(credit_check_task) == 2
    assert queue.dequeue().provider == COMPANIES_HOUSE_PROVIDER.name


def test_dequeue_respects_task_priority(queue):
    credit_check_task_one = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    credit_check_task_two = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=234, timestamp=datetime1, metadata={ "priority": Priority.HIGH })

    queue.enqueue(credit_check_task_one)
    queue.enqueue(credit_check_task_two)

    assert queue.dequeue().user_id == 234


def test_dequeue_respects_rule_of_three(queue):
    credit_check_task_one = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    bank_statements_task_one = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    id_verification_task_one = TaskSubmission(provider=ID_VERIFICATION_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    credit_check_task_two = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=234, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })

    queue.enqueue(credit_check_task_one)
    queue.enqueue(credit_check_task_two)
    queue.enqueue(bank_statements_task_one)
    queue.enqueue(id_verification_task_one)

    assert queue.dequeue().user_id == 123
    assert queue.dequeue().user_id == 123
    assert queue.dequeue().user_id == 123
    assert queue.dequeue().user_id == 123
    assert queue.dequeue().user_id == 234


def test_respects_timestamp_ordering(queue):
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    id_verification_task = TaskSubmission(provider=ID_VERIFICATION_PROVIDER.name, user_id=234, timestamp=datetime2, metadata={ "priority": Priority.NORMAL })

    queue.enqueue(credit_check_task)
    queue.enqueue(id_verification_task)

    assert queue.dequeue().user_id == 234


def test_deduplication_logic(queue):
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    bank_statement_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.NORMAL })
    duplicate_credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.NORMAL })

    queue.enqueue(credit_check_task)
    queue.enqueue(bank_statement_task)

    assert queue.enqueue(duplicate_credit_check_task) == 3
    assert queue.dequeue().provider == COMPANIES_HOUSE_PROVIDER.name


def test_deduplication_with_dependencies(queue):
    companies_house_task = TaskSubmission(provider=COMPANIES_HOUSE_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.NORMAL })
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    bank_statement_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.NORMAL })
    duplicate_credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.NORMAL })

    queue.enqueue(companies_house_task)
    queue.enqueue(credit_check_task)
    queue.enqueue(bank_statement_task)

    assert queue.enqueue(duplicate_credit_check_task) == 3
    assert queue.dequeue().provider == COMPANIES_HOUSE_PROVIDER.name
    assert queue.dequeue().provider == CREDIT_CHECK_PROVIDER.name
    assert queue.dequeue().provider == BANK_STATEMENTS_PROVIDER.name


def test_deprioritisation_of_bank_statements_rule_of_three(queue):
    bank_statements_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.HIGH })
    credit_check_task = TaskSubmission(provider=CREDIT_CHECK_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })
    id_verification_task = TaskSubmission(provider=ID_VERIFICATION_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })

    queue.enqueue(bank_statements_task)
    queue.enqueue(credit_check_task)
    queue.enqueue(id_verification_task)

    assert queue.dequeue().provider == COMPANIES_HOUSE_PROVIDER.name
    assert queue.dequeue().provider == CREDIT_CHECK_PROVIDER.name
    assert queue.dequeue().provider == ID_VERIFICATION_PROVIDER.name
    assert queue.dequeue().provider == BANK_STATEMENTS_PROVIDER.name


def test_deprioritisation_of_bank_statements_general(queue):
    bank_statements_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.HIGH })
    id_verification_task = TaskSubmission(provider=ID_VERIFICATION_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })

    queue.enqueue(bank_statements_task)
    queue.enqueue(id_verification_task)

    assert queue.dequeue().provider == ID_VERIFICATION_PROVIDER.name
    assert queue.dequeue().provider == BANK_STATEMENTS_PROVIDER.name


def test_queue_age(queue):
    assert queue.age == 0

    bank_statements_task = TaskSubmission(provider=BANK_STATEMENTS_PROVIDER.name, user_id=123, timestamp=datetime2, metadata={ "priority": Priority.HIGH })
    id_verification_task = TaskSubmission(provider=ID_VERIFICATION_PROVIDER.name, user_id=123, timestamp=datetime1, metadata={ "priority": Priority.NORMAL })

    queue.enqueue(bank_statements_task)
    queue.enqueue(id_verification_task)

    assert queue.age == 3600

