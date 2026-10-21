import asyncio
import inspect
import logging
from typing import Any, Callable, Dict, List, Optional
from .models import Job
from .queue import JobQueue

logger = logging.getLogger("smart_job_queue")


class WorkerPool:
    """Async worker pool that polls jobs, dispatches to topic handlers, and applies retries."""

    def __init__(self, queue: JobQueue, concurrency: int = 2, poll_interval: float = 0.05):
        self.queue = queue
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self._handlers: Dict[str, Callable[[Job], Any]] = {}
        self._running = False
        self._tasks: List[asyncio.Task] = []

    def register(self, topic: str, handler: Callable[[Job], Any]) -> None:
        self._handlers[topic] = handler

    async def _worker_loop(self, worker_id: int) -> None:
        topics = list(self._handlers.keys())
        while self._running:
            job = self.queue.storage.fetch_next_job(topics=topics if topics else None)
            if not job:
                await asyncio.sleep(self.poll_interval)
                continue

            handler = self._handlers.get(job.topic)
            if not handler:
                self.queue.storage.update_job_failure(job, f"No handler registered for topic: {job.topic}")
                continue

            try:
                if inspect.iscoroutinefunction(handler):
                    await handler(job)
                else:
                    handler(job)
                self.queue.storage.update_job_success(job.id)
            except Exception as e:
                self.queue.storage.update_job_failure(job, str(e))

    def start(self) -> None:
        self._running = True
        for i in range(self.concurrency):
            t = asyncio.create_task(self._worker_loop(i))
            self._tasks.append(t)

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
