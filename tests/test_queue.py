import asyncio
import unittest
from smart_job_queue import JobQueue, WorkerPool, JobPriority, JobStatus


class TestSmartJobQueue(unittest.TestCase):
    def test_priority_ordering(self):
        queue = JobQueue(db_path=":memory:")
        queue.enqueue("task", {"name": "low"}, priority=JobPriority.LOW)
        queue.enqueue("task", {"name": "critical"}, priority=JobPriority.CRITICAL)
        queue.enqueue("task", {"name": "normal"}, priority=JobPriority.NORMAL)

        # Critical must be fetched first
        j1 = queue.storage.fetch_next_job()
        self.assertIsNotNone(j1)
        self.assertEqual(j1.payload["name"], "critical")

        # Normal next
        j2 = queue.storage.fetch_next_job()
        self.assertIsNotNone(j2)
        self.assertEqual(j2.payload["name"], "normal")

        # Low last
        j3 = queue.storage.fetch_next_job()
        self.assertIsNotNone(j3)
        self.assertEqual(j3.payload["name"], "low")

    def test_worker_processing_and_dlq(self):
        async def run_test():
            queue = JobQueue(db_path=":memory:")
            processed = []

            async def handle_ping(job):
                processed.append(job.payload["msg"])

            async def handle_failing(job):
                raise ValueError("Permanent failure")

            pool = WorkerPool(queue, concurrency=1, poll_interval=0.01)
            pool.register("ping", handle_ping)
            pool.register("fail", handle_failing)

            queue.enqueue("ping", {"msg": "hello"})
            # max_retries=1 means it immediately goes to DEAD_LETTER after 1 failure
            queue.enqueue("fail", {"msg": "fail"}, max_retries=1)

            pool.start()
            await asyncio.sleep(0.1)
            await pool.stop()

            self.assertIn("hello", processed)
            stats = queue.stats()
            self.assertEqual(stats["completed"], 1)
            self.assertEqual(stats["dead_letter"], 1)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
