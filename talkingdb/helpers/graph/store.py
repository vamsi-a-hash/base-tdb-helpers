"""SQLite persistence helpers for graph cleanup.

Persistence is "HOW" (Synterex Engineering Handbook Section 12.5), so the
SQL lives here in helpers rather than on the entity class in
base-tdb-models. ``GraphModel`` stays a pure entity definition.
"""

import sqlite3
from typing import Optional


def delete(conn: sqlite3.Connection, graph_id: Optional[str]) -> None:
    """Remove all persisted graph data for a graph ID.

    The operation is intentionally idempotent:
    - ``None`` or empty graph IDs are ignored
    - deleting a non-existent graph is treated as success

    Used during rollback/cleanup flows for:
    - cancelled ingestion jobs
    - failed indexing/training jobs
    - orphaned partial graph creation
    """
    if not graph_id:
        return

    with conn:
        conn.execute(
            "DELETE FROM edges WHERE graph_id = ?",
            (graph_id,),
        )
        conn.execute(
            "DELETE FROM nodes WHERE graph_id = ?",
            (graph_id,),
        )
