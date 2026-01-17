import pytest
from solutions.IWC.queue_solution_legacy import Queue

@pytest.fixture
def queue():
    return Queue()

def test_age(queue):
    assert queue.age == 0

def test_size_empty_queue(queue):
    assert queue.size == 0

def test_size_busy_queue(queue):
    queue.enqueue

