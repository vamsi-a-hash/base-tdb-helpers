import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from talkingdb.models.job.error import JobErrorCode
from talkingdb.models.job.job import JobModel
from talkingdb.models.job.stage import JobStage
from talkingdb.models.job.state import JobState

_TERMINAL = tuple(s.value for s in JobState.terminal())
_TERMINAL_PLACEHOLDERS = ",".join("?" for _ in _TERMINAL)


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Used for all persisted lifecycle timestamps so every job row follows
    a single consistent time representation across creation, progress,
    heartbeats, cancellation, completion, and retention logic.
    """
    return datetime.now(timezone.utc).isoformat()


def _dumps(value: Optional[Dict[str, Any]]) -> Optional[str]:
    """Serialize a structured payload to JSON for SQLite storage.

    Job metadata such as ``result_summary`` and ``progress_details`` are
    stored as TEXT columns in SQLite and converted back to dictionaries
    when read into the model layer.
    """
    return json.dumps(value) if value is not None else None


def _loads(value: Optional[str]) -> Optional[Dict[str, Any]]:
    """Deserialize a JSON payload stored in SQLite.

    Returns ``None`` for empty or NULL database values so optional
    structured fields map cleanly back into the ``JobModel``.
    """
    return json.loads(value) if value else None


# --------------------------------------------------------------------- schema
def init_db(conn: sqlite3.Connection) -> None:
    """Create the jobs table and supporting indexes (idempotent)."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id           TEXT PRIMARY KEY,
            idempotency_key  TEXT,
            state            TEXT NOT NULL,
            stage            TEXT,
            total_units      INTEGER DEFAULT 0,
            done_units       INTEGER DEFAULT 0,
            cancel_requested INTEGER DEFAULT 0,
            result_graph_id  TEXT,
            result_summary   TEXT,
            progress_details TEXT,
            status_message   TEXT,
            error_code       TEXT,
            error_message    TEXT,
            user_id          TEXT,
            session_id       TEXT,
            filename         TEXT,
            file_size_bytes  INTEGER,
            temp_path        TEXT,
            heartbeat_at     TEXT,
            started_at       TEXT,
            completed_at     TEXT,
            created_at       TEXT,
            updated_at       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_idem  ON jobs(idempotency_key);
        CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
        """
    )


# ----------------------------------------------------------------- row <-> model
def _row_to_job(row: sqlite3.Row) -> JobModel:
    """Convert a SQLite row into a ``JobModel``.

    Centralizes all row-to-model mapping so enum conversion, JSON decoding,
    and nullable handling stay consistent across every query path.
    """
    return JobModel(
        job_id=row["job_id"],
        idempotency_key=row["idempotency_key"],
        state=JobState(row["state"]),
        stage=JobStage(row["stage"]) if row["stage"] else None,
        total_units=row["total_units"] or 0,
        done_units=row["done_units"] or 0,
        cancel_requested=bool(row["cancel_requested"]),
        result_graph_id=row["result_graph_id"],
        result_summary=_loads(row["result_summary"]),
        progress_details=_loads(row["progress_details"]),
        status_message=row["status_message"],
        error_code=JobErrorCode(row["error_code"]) if row["error_code"] else None,
        error_message=row["error_message"],
        user_id=row["user_id"],
        session_id=row["session_id"],
        filename=row["filename"],
        file_size_bytes=row["file_size_bytes"],
        temp_path=row["temp_path"],
        heartbeat_at=row["heartbeat_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        created_at=row["created_at"] or "",
        updated_at=row["updated_at"] or "",
    )


# ----------------------------------------------------------------------- writes
def insert(conn: sqlite3.Connection, job: JobModel) -> None:
    """Persist a freshly created QUEUED job."""
    conn.execute(
        """
        INSERT INTO jobs (
            job_id, idempotency_key, state, stage,
            total_units, done_units, cancel_requested,
            user_id, session_id, filename, file_size_bytes, temp_path,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job.job_id,
            job.idempotency_key,
            job.state.value,
            job.stage.value if job.stage else None,
            job.total_units,
            job.done_units,
            1 if job.cancel_requested else 0,
            job.user_id,
            job.session_id,
            job.filename,
            job.file_size_bytes,
            job.temp_path,
            job.created_at,
            job.updated_at,
        ),
    )


def mark_ongoing(conn: sqlite3.Connection, job_id: str, started_at: str) -> bool:
    """QUEUED -> ONGOING. State-guarded; returns True if this call won."""
    cur = conn.execute(
        """
        UPDATE jobs
           SET state = ?, stage = ?, started_at = ?, updated_at = ?
         WHERE job_id = ? AND state = ?
        """,
        (
            JobState.ONGOING.value,
            JobStage.VALIDATING.value,
            started_at,
            _now_iso(),
            job_id,
            JobState.QUEUED.value,
        ),
    )
    return cur.rowcount > 0


def update_progress(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    stage: Optional[JobStage] = None,
    done_units: Optional[int] = None,
    total_units: Optional[int] = None,
    status_message: Optional[str] = None,
    progress_details: Optional[Dict[str, Any]] = None,
    heartbeat: bool = True,
) -> None:
    """Best-effort progress write.

    State-guarded so it can never resurrect a terminal job. Only the provided
    fields are touched; ``heartbeat`` stamps ``heartbeat_at`` so the orphan
    sweep can tell a live job from a dead one.
    """
    sets: List[str] = ["updated_at = ?"]
    params: List[Any] = [_now_iso()]

    if stage is not None:
        sets.append("stage = ?")
        params.append(stage.value)
    if done_units is not None:
        sets.append("done_units = ?")
        params.append(done_units)
    if total_units is not None:
        sets.append("total_units = ?")
        params.append(total_units)
    if status_message is not None:
        sets.append("status_message = ?")
        params.append(status_message)
    if progress_details is not None:
        sets.append("progress_details = ?")
        params.append(_dumps(progress_details))
    if heartbeat:
        sets.append("heartbeat_at = ?")
        params.append(_now_iso())

    params.append(job_id)
    conn.execute(
        f"UPDATE jobs SET {', '.join(sets)} "
        f"WHERE job_id = ? AND state NOT IN ({_TERMINAL_PLACEHOLDERS})",
        (*params, *_TERMINAL),
    )


def request_cancel(conn: sqlite3.Connection, job_id: str) -> Optional[JobModel]:
    """Cooperative cancel.

    * QUEUED  -> CANCELLED immediately (work never started).
    * ONGOING -> CANCELLING + cancel_requested flag (worker finalizes later).
    * terminal/CANCELLING -> no-op (idempotent).

    Returns the job's current state after the call, or None if unknown.
    """
    job = get(conn, job_id)
    if job is None:
        return None

    if job.state == JobState.QUEUED:
        conn.execute(
            """
            UPDATE jobs
               SET state = ?, cancel_requested = 1,
                   status_message = ?, completed_at = ?, updated_at = ?
             WHERE job_id = ? AND state = ?
            """,
            (
                JobState.CANCELLED.value,
                "Upload cancelled before processing started",
                _now_iso(),
                _now_iso(),
                job_id,
                JobState.QUEUED.value,
            ),
        )
    elif job.state == JobState.ONGOING:
        conn.execute(
            """
            UPDATE jobs
               SET state = ?, cancel_requested = 1,
                   status_message = ?, updated_at = ?
             WHERE job_id = ? AND state = ?
            """,
            (
                JobState.CANCELLING.value,
                "Cancelling upload...",
                _now_iso(),
                job_id,
                JobState.ONGOING.value,
            ),
        )
    # CANCELLING or terminal: nothing to do - idempotent no-op.

    return get(conn, job_id)


def finalize(
    conn: sqlite3.Connection,
    job_id: str,
    terminal_state: JobState,
    *,
    result_graph_id: Optional[str] = None,
    result_summary: Optional[Dict[str, Any]] = None,
    error_code: Optional[JobErrorCode] = None,
    error_message: Optional[str] = None,
    status_message: Optional[str] = None,
) -> bool:
    """Apply the single terminal transition.

    State-guarded (``state NOT IN terminal``) so exactly one caller - the
    worker OR the orphan sweep - can win. ``stage`` is cleared to NULL per the
    frozen invariant that a terminal job has no stage. ``progress_details`` is
    cleared (non-contractual, live-only).

    Returns True if this call performed the transition.
    """
    if not terminal_state.is_terminal():
        raise ValueError(f"{terminal_state} is not a terminal state")

    now = _now_iso()
    cur = conn.execute(
        f"""
        UPDATE jobs
           SET state = ?, stage = NULL, progress_details = NULL,
               result_graph_id = COALESCE(?, result_graph_id),
               result_summary  = ?,
               error_code      = ?,
               error_message   = ?,
               status_message  = COALESCE(?, status_message),
               completed_at    = ?, updated_at = ?
         WHERE job_id = ? AND state NOT IN ({_TERMINAL_PLACEHOLDERS})
        """,
        (
            terminal_state.value,
            result_graph_id,
            _dumps(result_summary),
            error_code.value if error_code else None,
            error_message,
            status_message,
            now,
            now,
            job_id,
            *_TERMINAL,
        ),
    )
    return cur.rowcount > 0


# ------------------------------------------------------------------------ reads
def get(conn: sqlite3.Connection, job_id: str) -> Optional[JobModel]:
    row = conn.execute(
        "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
    ).fetchone()
    return _row_to_job(row) if row else None


def find_by_idempotency_key(
    conn: sqlite3.Connection, key: str
) -> Optional[JobModel]:
    """Return the existing job for an idempotency key, if any.

    Any non-purged job (in-flight OR terminal) is returned, so a retry with
    the same key never creates a duplicate. Once retention purges the row the
    key becomes reusable and a fresh job is created.
    """
    if not key:
        return None
    row = conn.execute(
        "SELECT * FROM jobs WHERE idempotency_key = ? "
        "ORDER BY created_at DESC LIMIT 1",
        (key,),
    ).fetchone()
    return _row_to_job(row) if row else None


def is_cancel_requested(conn: sqlite3.Connection, job_id: str) -> bool:
    """Cheap read used at every checkpoint."""
    row = conn.execute(
        "SELECT cancel_requested FROM jobs WHERE job_id = ?", (job_id,)
    ).fetchone()
    return bool(row["cancel_requested"]) if row else False


def set_result_graph_id(
    conn: sqlite3.Connection, job_id: str, graph_id: str
) -> None:
    """Record the graph the worker is currently building.

    Written as soon as the indexer is constructed, so the orphan sweep can
    roll back partial graph data even if the worker process dies before
    finalizing. State-guarded so a terminal row is never resurrected.
    """
    conn.execute(
        f"UPDATE jobs SET result_graph_id = ?, updated_at = ? "
        f"WHERE job_id = ? AND state NOT IN ({_TERMINAL_PLACEHOLDERS})",
        (graph_id, _now_iso(), job_id, *_TERMINAL),
    )


# ----------------------------------------------------------------- sweeps
_NON_TERMINAL = (
    JobState.QUEUED.value,
    JobState.ONGOING.value,
    JobState.CANCELLING.value,
)
_NON_TERMINAL_PLACEHOLDERS = ",".join("?" for _ in _NON_TERMINAL)


def select_orphan_candidates(
    conn: sqlite3.Connection, stale_before_iso: str
) -> List[JobModel]:
    """Non-terminal jobs whose most recent liveness signal is older than the
    stale threshold.

    ``COALESCE(heartbeat_at, started_at, created_at)`` lets us detect a
    worker that died at any point: a job that never started (still QUEUED),
    one that started but never beat (died early), and one that beat then
    stopped beating are all caught by the same query.
    """
    rows = conn.execute(
        f"""
        SELECT * FROM jobs
         WHERE state IN ({_NON_TERMINAL_PLACEHOLDERS})
           AND COALESCE(heartbeat_at, started_at, created_at) < ?
        """,
        (*_NON_TERMINAL, stale_before_iso),
    ).fetchall()
    return [_row_to_job(r) for r in rows]


def select_timeout_candidates(
    conn: sqlite3.Connection, deadline_iso: str
) -> List[JobModel]:
    """Jobs whose ``started_at`` is older than the max-duration deadline.

    Only running states are considered - a QUEUED job has not consumed any
    processing budget yet. Only kicks in if the worker-side check missed
    (e.g. wedged inside the parser).
    """
    rows = conn.execute(
        """
        SELECT * FROM jobs
         WHERE state IN (?, ?)
           AND started_at IS NOT NULL
           AND started_at < ?
        """,
        (JobState.ONGOING.value, JobState.CANCELLING.value, deadline_iso),
    ).fetchall()
    return [_row_to_job(r) for r in rows]


def select_retention_expired(
    conn: sqlite3.Connection,
    *,
    completed_before_iso: str,
    failed_before_iso: str,
    cancelled_before_iso: str,
) -> List[JobModel]:
    """Terminal jobs old enough to be purged.

    Each terminal state has its own retention window, applied against the
    ``completed_at`` timestamp (set by ``finalize`` / cancel-while-queued).
    """
    rows = conn.execute(
        """
        SELECT * FROM jobs
         WHERE (state = ? AND completed_at < ?)
            OR (state = ? AND completed_at < ?)
            OR (state = ? AND completed_at < ?)
        """,
        (
            JobState.COMPLETED.value, completed_before_iso,
            JobState.FAILED.value, failed_before_iso,
            JobState.CANCELLED.value, cancelled_before_iso,
        ),
    ).fetchall()
    return [_row_to_job(r) for r in rows]


def delete(conn: sqlite3.Connection, job_id: str) -> None:
    """Hard-delete a job row (used by retention)."""
    conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))


def select_referenced_temp_paths(conn: sqlite3.Connection) -> set[str]:
    """Every temp path still referenced by a (non-deleted) job row."""
    rows = conn.execute(
        "SELECT temp_path FROM jobs WHERE temp_path IS NOT NULL"
    ).fetchall()
    return {r["temp_path"] for r in rows if r["temp_path"]}
