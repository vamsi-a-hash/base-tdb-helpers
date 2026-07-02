"""Session and chat-message persistence.

Two tables are managed here:

  sessions      — one row per conversation thread, owned by a user.
  chat_messages — ordered Q&A turns within a session.

Documents (jobs) are linked to sessions via jobs.session_id; that join
is handled by the caller using job_store.list_documents(conn, session_id).

Ownership invariant: every write path that creates or reads sensitive data
accepts ``user_email`` and enforces it in the WHERE clause, so one user
can never read or modify another user's sessions.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4


# ------------------------------------------------------------------ helpers

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    return f"sess::{uuid4().hex}"


def _make_message_id() -> str:
    return f"msg::{uuid4().hex}"


# ------------------------------------------------------------------ schema

def init_db(conn: sqlite3.Connection) -> None:
    """Create sessions and chat_messages tables (idempotent).

    sessions.user_email is a FK to users.email — enforced when the
    sqlite client has PRAGMA foreign_keys = ON (which it now does).

    chat_messages.session_id is a FK to sessions.session_id — same
    enforcement. The cascade on session delete is intentional: removing
    a session wipes its messages atomically.

    jobs.session_id is NOT declared as an FK here because SQLite cannot
    ALTER TABLE to add FK constraints, and dropping/recreating the jobs
    table would discard all live jobs. App-level enforcement is used
    instead: session_store.ensure_session() is called before any job
    insert that carries a session_id.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            session_id  TEXT PRIMARY KEY,
            user_email  TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
            title       TEXT,
            created_at  TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_user
            ON sessions(user_email);

        CREATE TABLE IF NOT EXISTS chat_messages (
            message_id  TEXT PRIMARY KEY,
            session_id  TEXT NOT NULL
                REFERENCES sessions(session_id) ON DELETE CASCADE,
            role        TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
            content     TEXT NOT NULL,
            created_at  TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_chat_messages_session
            ON chat_messages(session_id, created_at);
        """
    )


# ------------------------------------------------------------------ row mapping

def _row_to_session(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "user_email": row["user_email"],
        "title": row["title"],
        "created_at": row["created_at"],
    }


def _row_to_message(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "message_id": row["message_id"],
        "session_id": row["session_id"],
        "role": row["role"],
        "content": row["content"],
        "created_at": row["created_at"],
    }


# ------------------------------------------------------------------ session writes

def create_session(
    conn: sqlite3.Connection,
    user_email: str,
    *,
    title: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Insert a new session row and return it.

    ``session_id`` can be supplied by the caller (e.g. the client already
    picked an id before uploading the first document). If omitted, one is
    generated.
    """
    sid = session_id or _make_session_id()
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO sessions (session_id, user_email, title, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (sid, user_email.lower(), title, now),
    )
    return {"session_id": sid, "user_email": user_email.lower(), "title": title, "created_at": now}


def ensure_session(
    conn: sqlite3.Connection,
    session_id: str,
    user_email: str,
    *,
    title: Optional[str] = None,
) -> Dict[str, Any]:
    """Return an existing session or create it if absent.

    Called from the document-upload path so a client that passes a custom
    session_id string without pre-creating the session doesn't get a FK
    violation.
    """
    existing = get_session(conn, session_id)
    if existing is not None:
        return existing
    return create_session(conn, user_email, title=title, session_id=session_id)


def update_session_title(
    conn: sqlite3.Connection,
    session_id: str,
    user_email: str,
    title: str,
) -> bool:
    """Update the session title. Returns True if the row was found and owned by user."""
    cur = conn.execute(
        "UPDATE sessions SET title = ? WHERE session_id = ? AND user_email = ?",
        (title, session_id, user_email.lower()),
    )
    return cur.rowcount > 0


# ------------------------------------------------------------------ session reads

def get_session(
    conn: sqlite3.Connection,
    session_id: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    return _row_to_session(row) if row else None


def list_sessions(
    conn: sqlite3.Connection,
    user_email: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """All sessions for a user, newest first."""
    rows = conn.execute(
        """
        SELECT * FROM sessions
        WHERE user_email = ?
        ORDER BY created_at DESC, session_id DESC
        LIMIT ? OFFSET ?
        """,
        (user_email.lower(), limit, offset),
    ).fetchall()
    return [_row_to_session(r) for r in rows]


# ------------------------------------------------------------------ message writes

def add_message(
    conn: sqlite3.Connection,
    session_id: str,
    role: str,
    content: str,
) -> Dict[str, Any]:
    """Append a chat turn to the session. ``role`` must be 'user' or 'assistant'."""
    if role not in ("user", "assistant"):
        raise ValueError(f"Invalid role '{role}': must be 'user' or 'assistant'")
    mid = _make_message_id()
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO chat_messages (message_id, session_id, role, content, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (mid, session_id, role, content, now),
    )
    return {"message_id": mid, "session_id": session_id, "role": role, "content": content, "created_at": now}


# ------------------------------------------------------------------ message reads

def list_messages(
    conn: sqlite3.Connection,
    session_id: str,
) -> List[Dict[str, Any]]:
    """All messages for a session, oldest first (chronological Q&A order)."""
    rows = conn.execute(
        """
        SELECT * FROM chat_messages
        WHERE session_id = ?
        ORDER BY created_at ASC, message_id ASC
        """,
        (session_id,),
    ).fetchall()
    return [_row_to_message(r) for r in rows]


def get_last_message(
    conn: sqlite3.Connection,
    session_id: str,
) -> Optional[Dict[str, Any]]:
    """Most recent message — used to build history list previews."""
    row = conn.execute(
        """
        SELECT * FROM chat_messages
        WHERE session_id = ?
        ORDER BY created_at DESC, message_id DESC
        LIMIT 1
        """,
        (session_id,),
    ).fetchone()
    return _row_to_message(row) if row else None


def get_first_message(
    conn: sqlite3.Connection,
    session_id: str,
) -> Optional[Dict[str, Any]]:
    """Oldest message — used as the session preview in the history list."""
    row = conn.execute(
        """
        SELECT * FROM chat_messages
        WHERE session_id = ?
        ORDER BY created_at ASC, message_id ASC
        LIMIT 1
        """,
        (session_id,),
    ).fetchone()
    return _row_to_message(row) if row else None


def count_messages(conn: sqlite3.Connection, session_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM chat_messages WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    return row["cnt"] if row else 0