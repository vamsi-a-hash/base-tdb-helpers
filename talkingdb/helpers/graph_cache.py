import threading
import time
from typing import Dict

from talkingdb.clients.sqlite import sqlite_conn
from talkingdb.models.graph.graph import GraphModel


class GraphModelCache:
    def __init__(self, ttl_seconds: int = 300, cleanup_interval: int = 120):
        self.ttl_seconds = ttl_seconds
        self.cleanup_interval = cleanup_interval

        self._cache: Dict[str, dict] = {}
        self._lock = threading.Lock()

        self._start_cleanup_worker()

    def get(self, graph_id: str) -> GraphModel:
        now = time.time()

        with self._lock:
            entry = self._cache.get(graph_id)
            if entry:
                entry["last_used"] = now
                return entry["graph_model"]

        with sqlite_conn() as conn:
            base_model = GraphModel.load(conn, graph_id)

        with self._lock:
            self._cache[graph_id] = {
                "graph_model": base_model,
                "last_used": now,
            }

        return base_model

    def invalidate(self, graph_id: str) -> None:
        """Clear the cached entry for a graph after updates or deletion."""
        with self._lock:
            self._cache.pop(graph_id, None)

    def _start_cleanup_worker(self):
        thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
        )
        thread.start()

    def _cleanup_loop(self):
        while True:
            time.sleep(self.cleanup_interval)
            self._cleanup()

    def _cleanup(self):
        now = time.time()
        expired = []

        with self._lock:
            for graph_id, entry in list(self._cache.items()):
                if now - entry["last_used"] > self.ttl_seconds:
                    expired.append(graph_id)

            for graph_id in expired:
                del self._cache[graph_id]

        if expired:
            print(f"[GraphModelCache] Evicted graphs: {expired}")


graph_cache = GraphModelCache(
    ttl_seconds=300,
    cleanup_interval=120,
)
