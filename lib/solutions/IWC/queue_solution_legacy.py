from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import Dict, Tuple, List

# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch

class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the legacy system."""
    HIGH = 1
    NORMAL = 2

@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]

MAX_TIMESTAMP = datetime.max.replace(tzinfo=None)

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[]
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[]
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[]
)


REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]

class Queue:
    def __init__(self):
        self._queue: Dict[Tuple[str, str], TaskSubmission] = {}
        self._deprioritised_providers: List[str] = [BANK_STATEMENTS_PROVIDER.name]

        self._oldest_task_timestamp: datetime | None = None
        self._newest_task_timestamp: datetime | None = None

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks

    def _should_deprioritise_task(self, task: TaskSubmission) -> bool:
        is_deprioritised_provider = task.provider in self._deprioritised_providers

        return is_deprioritised_provider

    def _should_reprioritise_deprioritised_task(self, task: TaskSubmission) -> bool:
        task_internal_age = int((self._newest_task_timestamp - self._timestamp_for_task(task)).total_seconds())
        is_task_internal_age_above_limit = task_internal_age >= 300

        return self._should_deprioritise_task(task) and is_task_internal_age_above_limit

    @staticmethod
    def _priority_for_task(task):
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task):
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
    
    @staticmethod
    def _complexity_weighting_for_task(task):
        metadata = task.metadata
        return metadata.get("complexity_weighting", 1)

    @staticmethod
    def _timestamp_for_task(task):
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp

    def enqueue(self, item: TaskSubmission) -> int:
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            task_key = (task.user_id, task.provider)

            existing_match = self._queue.get(task_key, None)

            if existing_match:
                if existing_match.timestamp < task.timestamp:
                    task.timestamp = existing_match.timestamp

            if self._oldest_task_timestamp is None or self._timestamp_for_task(task) < self._oldest_task_timestamp:
                self._oldest_task_timestamp = self._timestamp_for_task(task)
            
            if self._newest_task_timestamp is None or self._timestamp_for_task(task) > self._newest_task_timestamp:
                self._newest_task_timestamp = self._timestamp_for_task(task)

            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
            metadata.setdefault("complexity_weighting", 1)
            self._queue[task_key] = task
        return self.size

    def dequeue(self):
        if self.size == 0:
            return None

        queued_tasks = list(self._queue.values())

        user_ids = {task.user_id for task in queued_tasks}
        task_count = {}
        priority_timestamps = {}
        user_lowest_priorities = {}

        for user_id in user_ids:
            user_tasks = [t for t in queued_tasks if t.user_id == user_id]
            earliest_timestamp = sorted(user_tasks, key=lambda t: t.timestamp)[0].timestamp
            user_lowest_priorities[user_id] = max(
                [self._priority_for_task(t) for t in user_tasks]
            )
            priority_timestamps[user_id] = earliest_timestamp
            task_count[user_id] = len(user_tasks)

        for task in queued_tasks:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            raw_priority = metadata.get("priority")

            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                if self._should_reprioritise_deprioritised_task(task):
                    metadata["priority"] = Priority.HIGH
                
                if task_count[task.user_id] >= 3:
                    metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level

                if self._should_deprioritise_task(task) and not self._should_reprioritise_deprioritised_task(task):
                    metadata["priority"] = user_lowest_priorities[task.user_id]

            if self._should_deprioritise_task(task) and not self._should_reprioritise_deprioritised_task(task):
                metadata["complexity_weighting"] = 2

        queued_tasks.sort(
            key=lambda i: (
                self._priority_for_task(i),
                self._earliest_group_timestamp_for_task(i),
                self._complexity_weighting_for_task(i),
                self._timestamp_for_task(i),
                self._should_reprioritise_deprioritised_task(i) == False
            )
        )

        task = queued_tasks[0]
        del self._queue[(task.user_id, task.provider)]

        if self._timestamp_for_task(task) == self._oldest_task_timestamp or self._timestamp_for_task(task) == self._newest_task_timestamp:
            if self.size == 0:
                self._oldest_task_timestamp = None
                self._newest_task_timestamp = None
            else:
                timestamps = [self._timestamp_for_task(t) for t in self._queue.values()]
                self._oldest_task_timestamp = min(timestamps)
                self._newest_task_timestamp = max(timestamps)

        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self):
        return len(self._queue.values())

    @property
    def age(self):
        if self.size == 0:
            return 0
        
        return int((self._newest_task_timestamp - self._oldest_task_timestamp).total_seconds())


    def purge(self):
        self._queue = {}
        return True

"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""

