from dataclasses import dataclass, field
import enum
import time
from typing import Any, Dict, Optional
import uuid


class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class JobPriority(int, enum.Enum):
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


@dataclass
class Job:
    id: str
    topic: str
    payload: Dict[str, Any]
    priority: int = JobPriority.NORMAL
    status: JobStatus = JobStatus.QUEUED
    attempts: int = 0
    max_retries: int = 3
    backoff_base: float = 2.0
    next_run_at: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    error_msg: Optional[str] = None

    @classmethod
    def create(
        cls,
        topic: str,
        payload: Dict[str, Any],
        priority: int = JobPriority.NORMAL,
        max_retries: int = 3,
        delay_seconds: float = 0.0,
    ) -> "Job":
        now = time.time()
        return cls(
            id=str(uuid.uuid4()),
            topic=topic,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
            next_run_at=now + delay_seconds,
            created_at=now,
        )
