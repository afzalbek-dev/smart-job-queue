from typing import Any, Dict, List, Optional
from .models import Job, JobPriority
from .storage import SQLiteJobStorage


class JobQueue:
    """High-level client for enqueuing and managing asynchronous background jobs."""

    def __init__(self, db_path: str = ":memory:"):
        self.storage = SQLiteJobStorage(db_path=db_path)

    def enqueue(
        self,
        topic: str,
        payload: Dict[str, Any],
        priority: int = JobPriority.NORMAL,
        max_retries: int = 3,
        delay_seconds: float = 0.0,
    ) -> Job:
        job = Job.create(
            topic=topic,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
            delay_seconds=delay_seconds,
        )
        self.storage.insert_job(job)
        return job

    def stats(self) -> dict:
        return self.storage.get_stats()
