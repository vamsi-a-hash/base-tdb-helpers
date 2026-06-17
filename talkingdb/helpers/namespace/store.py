import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Reserved, publicly readable namespace for the first-run demo experience.
DEMO_NAMESPACE = "demo-library"

_DEMO_TITLE = "Demo Library"
_DEMO_DESCRIPTION = (
    "Ready-made sample documents you can explore without signing in."
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(conn: sqlite3.Connection) -> None:
    """Create the namespaces table (idempotent)."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS namespaces (
            namespace    TEXT PRIMARY KEY,
            title        TEXT,
            description  TEXT,
            public_read  INTEGER NOT NULL DEFAULT 0,
            created_at   TEXT,
            updated_at   TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_namespaces_public
            ON namespaces(public_read);
        """
    )


# ----------------------------------------------------------------- row mapping
def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    """Map a namespaces row to a plain dict (public_read as bool)."""
    return {
        "namespace": row["namespace"],
        "title": row["title"],
        "description": row["description"],
        "public_read": bool(row["public_read"]),
        "created_at": row["created_at"] or "",
        "updated_at": row["updated_at"] or "",
    }


# ----------------------------------------------------------------------- writes
def upsert_namespace(
    conn: sqlite3.Connection,
    namespace: str,
    *,
    title: Optional[str] = None,
    description: Optional[str] = None,
    public_read: bool = False,
) -> Dict[str, Any]:
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO namespaces (
            namespace, title, description, public_read, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(namespace) DO UPDATE SET
            title       = excluded.title,
            description = excluded.description,
            public_read = excluded.public_read,
            updated_at  = excluded.updated_at
        """,
        (namespace, title, description, 1 if public_read else 0, now, now),
    )
    return get_namespace(conn, namespace)


def ensure_reserved(conn: sqlite3.Connection) -> None:
    """Seed the reserved ``demo-library`` namespace if it does not exist."""
    if get_namespace(conn, DEMO_NAMESPACE) is not None:
        return
    now = _now_iso()
    conn.execute(
        """
        INSERT OR IGNORE INTO namespaces (
            namespace, title, description, public_read, created_at, updated_at
        ) VALUES (?, ?, ?, 1, ?, ?)
        """,
        (DEMO_NAMESPACE, _DEMO_TITLE, _DEMO_DESCRIPTION, now, now),
    )


# ------------------------------------------------------------------------ reads
def get_namespace(
    conn: sqlite3.Connection, namespace: str
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM namespaces WHERE namespace = ?", (namespace,)
    ).fetchone()
    return _row_to_dict(row) if row else None


def list_namespaces(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM namespaces ORDER BY namespace ASC"
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def is_public(conn: sqlite3.Connection, namespace: str) -> bool:
    row = conn.execute(
        "SELECT public_read FROM namespaces WHERE namespace = ?", (namespace,)
    ).fetchone()
    return bool(row["public_read"]) if row else False
