from typing import Any, Dict, List, Optional
from .models import Job, JobPriority
from .storage import SQLiteJobStorage


class JobQueue:
    """High-level client for enqueuing, inspecting, and managing asynchronous background jobs."""

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

    def get_dead_letter_jobs(self, limit: int = 50) -> List[Job]:
        """Fetch failed jobs currently residing in Dead-Letter Queue (DLQ)."""
        return self.storage.get_dead_letter_jobs(limit=limit)

    def retry_dead_letter_job(self, job_id: str) -> bool:
        """Retry a failed dead-letter job by re-queuing it immediately."""
        return self.storage.retry_dead_letter_job(job_id=job_id)

    def stats(self) -> dict:
        return self.storage.get_stats()
