"""Graph cleanup helpers."""

from typing import Optional

from talkingdb.clients.sqlite import sqlite_conn
from talkingdb.helpers.graph import store as graph_store


def rollback_graph(graph_id: Optional[str]) -> None:
    """Remove all graph data associated with a graph ID.

    The operation is idempotent and safe to call multiple times.
    """
    if not graph_id:
        return
    with sqlite_conn() as conn:
        graph_store.delete(conn, graph_id)
