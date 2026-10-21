import json
import sqlite3
import threading
import time
from typing import List, Optional
from .models import Job, JobPriority, JobStatus


class SQLiteJobStorage:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._shared_conn = sqlite3.connect(db_path, check_same_thread=False) if db_path == ":memory:" else None
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self._shared_conn is not None:
            return self._shared_conn
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                        id TEXT PRIMARY KEY,
                        topic TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        priority INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL,
                        max_retries INTEGER NOT NULL,
                        backoff_base REAL NOT NULL,
                        next_run_at REAL NOT NULL,
                        created_at REAL NOT NULL,
                        error_msg TEXT
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_ready_jobs ON jobs(status, next_run_at, priority)")
                conn.commit()
            finally:
                if self._shared_conn is None:
                    conn.close()

    def insert_job(self, job: Job) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute(
                    """
                    INSERT INTO jobs (id, topic, payload, priority, status, attempts, max_retries, backoff_base, next_run_at, created_at, error_msg)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.id,
                        job.topic,
                        json.dumps(job.payload),
                        job.priority,
                        job.status.value,
                        job.attempts,
                        job.max_retries,
                        job.backoff_base,
                        job.next_run_at,
                        job.created_at,
                        job.error_msg,
                    ),
                )
                conn.commit()
            finally:
                if self._shared_conn is None:
                    conn.close()

    def fetch_next_job(self, topics: Optional[List[str]] = None) -> Optional[Job]:
        now = time.time()
        with self._lock:
            conn = self._get_connection()
            try:
                query = """
                    SELECT id, topic, payload, priority, status, attempts, max_retries, backoff_base, next_run_at, created_at, error_msg
                    FROM jobs
                    WHERE status = 'QUEUED' AND next_run_at <= ?
                """
                params = [now]
                if topics:
                    placeholders = ",".join("?" for _ in topics)
                    query += f" AND topic IN ({placeholders})"
                    params.extend(topics)

                query += " ORDER BY priority DESC, created_at ASC LIMIT 1"

                cur = conn.execute(query, params)
                row = cur.fetchone()
                if not row:
                    return None

                job = Job(
                    id=row[0],
                    topic=row[1],
                    payload=json.loads(row[2]),
                    priority=row[3],
                    status=JobStatus(row[4]),
                    attempts=row[5],
                    max_retries=row[6],
                    backoff_base=row[7],
                    next_run_at=row[8],
                    created_at=row[9],
                    error_msg=row[10],
                )

                # Mark as PROCESSING immediately
                conn.execute("UPDATE jobs SET status = 'PROCESSING' WHERE id = ?", (job.id,))
                conn.commit()
                job.status = JobStatus.PROCESSING
                return job
            finally:
                if self._shared_conn is None:
                    conn.close()

    def update_job_success(self, job_id: str) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE jobs SET status = 'COMPLETED', error_msg = NULL WHERE id = ?", (job_id,))
                conn.commit()
            finally:
                if self._shared_conn is None:
                    conn.close()

    def update_job_failure(self, job: Job, error_msg: str) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                job.attempts += 1
                job.error_msg = error_msg
                if job.attempts >= job.max_retries:
                    job.status = JobStatus.DEAD_LETTER
                else:
                    job.status = JobStatus.QUEUED
                    delay = job.backoff_base ** job.attempts
                    job.next_run_at = time.time() + delay

                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, attempts = ?, next_run_at = ?, error_msg = ?
                    WHERE id = ?
                    """,
                    (job.status.value, job.attempts, job.next_run_at, job.error_msg, job.id),
                )
                conn.commit()
            finally:
                if self._shared_conn is None:
                    conn.close()

    def get_dead_letter_jobs(self, limit: int = 50) -> List[Job]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    SELECT id, topic, payload, priority, status, attempts, max_retries, backoff_base, next_run_at, created_at, error_msg
                    FROM jobs
                    WHERE status = 'DEAD_LETTER'
                    ORDER BY created_at DESC LIMIT ?
                    """,
                    (limit,),
                )
                jobs = []
                for row in cur.fetchall():
                    jobs.append(
                        Job(
                            id=row[0],
                            topic=row[1],
                            payload=json.loads(row[2]),
                            priority=row[3],
                            status=JobStatus(row[4]),
                            attempts=row[5],
                            max_retries=row[6],
                            backoff_base=row[7],
                            next_run_at=row[8],
                            created_at=row[9],
                            error_msg=row[10],
                        )
                    )
                return jobs
            finally:
                if self._shared_conn is None:
                    conn.close()

    def retry_dead_letter_job(self, job_id: str) -> bool:
        now = time.time()
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'QUEUED', attempts = 0, next_run_at = ?, error_msg = NULL
                    WHERE id = ? AND status = 'DEAD_LETTER'
                    """,
                    (now, job_id),
                )
                conn.commit()
                return cur.rowcount > 0
            finally:
                if self._shared_conn is None:
                    conn.close()

    def get_stats(self) -> dict:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.execute("SELECT status, count(*) FROM jobs GROUP BY status")
                counts = {row[0]: row[1] for row in cur.fetchall()}
                return {
                    "queued": counts.get("QUEUED", 0),
                    "processing": counts.get("PROCESSING", 0),
                    "completed": counts.get("COMPLETED", 0),
                    "dead_letter": counts.get("DEAD_LETTER", 0),
                }
            finally:
                if self._shared_conn is None:
                    conn.close()
