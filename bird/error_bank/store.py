"""ErrorBankStore: 4-layer hybrid index.

I₁: HashMap  qid → [ErrorEntry]              (instance level)
I₂: HashMap  (db_id, column) → [ErrorEntry]  (column level, covers A+B types)
I₃: HashMap  (db_id, error_type) → [ErrorEntry]  (schema level)
I₄: HNSW     embedding → [ErrorEntry]        (semantic level, optional)
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from error_bank.schema import ErrorEntry, ErrorCategory
from error_bank.confidence import ConfidenceTracker


class ErrorBankStore:
    """Four-layer hybrid error memory store."""

    def __init__(self):
        # I₁: instance index — question_id → [ErrorEntry]
        self.i1_by_qid: Dict[int, List[ErrorEntry]] = defaultdict(list)

        # I₂: column index — (db_id, "table.column" or "table") → [ErrorEntry]
        self.i2_by_db_col: Dict[Tuple[str, str], List[ErrorEntry]] = defaultdict(list)

        # I₃: db-error-type index — (db_id, error_type) → [ErrorEntry]
        self.i3_by_db_err: Dict[Tuple[str, str], List[ErrorEntry]] = defaultdict(list)

        # I₄: semantic index (HNSW, lazily initialized)
        self.i4_entries: List[ErrorEntry] = []
        self.i4_vectors: List[Any] = []
        self._hnsw_index = None
        self._embed_fn = None

        # Confidence tracker
        self.confidence = ConfidenceTracker()

        # Thread safety
        self._lock = threading.Lock()

        # Stats
        self._insert_count = 0

    # ── Insert ──────────────────────────────────────────────────

    def insert(self, entry: ErrorEntry):
        """Insert an error entry into all applicable index layers."""
        with self._lock:
            self._insert_count += 1

            # I₁: always index by question_id
            self.i1_by_qid[entry.question_id].append(entry)

            # I₂: index by (db_id, column) for each involved column/table
            for col in entry.columns:
                key = (entry.db_id, col)
                self.i2_by_db_col[key].append(entry)

            for table in entry.tables:
                key = (entry.db_id, table)
                self.i2_by_db_col[key].append(entry)

            # Also index wrong names (for schema errors)
            for name in entry.wrong_names:
                key = (entry.db_id, name)
                self.i2_by_db_col[key].append(entry)

            # I₃: index by (db_id, error_type)
            key3 = (entry.db_id, entry.error_type.value)
            self.i3_by_db_err[key3].append(entry)

            # I₄: semantic index (only for structural errors)
            if entry.category == ErrorCategory.STRUCTURAL and self._embed_fn:
                vec = self._embed_fn(f"{entry.question} | {entry.error_detail}")
                self.i4_entries.append(entry)
                self.i4_vectors.append(vec)
                # Rebuild HNSW periodically
                if len(self.i4_entries) % 50 == 0:
                    self._rebuild_hnsw()

            # Update confidence
            for col in entry.columns:
                self.confidence.observe(entry.db_id, col, entry.error_type.value, is_positive=True)
            for table in entry.tables:
                self.confidence.observe(entry.db_id, table, entry.error_type.value, is_positive=True)

    def insert_batch(self, entries: List[ErrorEntry]):
        for e in entries:
            self.insert(e)

    # ── Query ───────────────────────────────────────────────────

    def query_by_qid(self, question_id: int) -> List[ErrorEntry]:
        """I₁: exact question match."""
        return list(self.i1_by_qid.get(question_id, []))

    def query_by_db_column(self, db_id: str, column: str) -> List[ErrorEntry]:
        """I₂: exact (db, column) match."""
        return list(self.i2_by_db_col.get((db_id, column), []))

    def query_by_db_table(self, db_id: str, table: str) -> List[ErrorEntry]:
        """I₂: exact (db, table) match."""
        return list(self.i2_by_db_col.get((db_id, table), []))

    def query_by_db_error_type(self, db_id: str, error_type: str) -> List[ErrorEntry]:
        """I₃: exact (db, error_type) match."""
        return list(self.i3_by_db_err.get((db_id, error_type), []))

    def query_semantic(self, query_text: str, db_id: str, k: int = 5) -> List[Tuple[ErrorEntry, float]]:
        """I₄: HNSW approximate nearest neighbor search."""
        if not self._hnsw_index or not self._embed_fn:
            return []
        vec = self._embed_fn(query_text)
        try:
            ids, dists = self._hnsw_index.knn_query(vec, k=min(k * 2, len(self.i4_entries)))
            results = []
            for idx, dist in zip(ids[0], dists[0]):
                entry = self.i4_entries[idx]
                if entry.db_id == db_id:  # only same-DB results
                    results.append((entry, float(dist)))
            return results[:k]
        except Exception:
            return []

    # ── HNSW management ─────────────────────────────────────────

    def set_embedding_fn(self, fn):
        """Set the embedding function for I₄ semantic layer."""
        self._embed_fn = fn

    def _rebuild_hnsw(self):
        """Rebuild HNSW index from current vectors."""
        if not self.i4_vectors:
            return
        try:
            import hnswlib
            dim = len(self.i4_vectors[0])
            index = hnswlib.Index(space='cosine', dim=dim)
            index.init_index(max_elements=max(len(self.i4_vectors) * 2, 100), ef_construction=200, M=16)
            for i, vec in enumerate(self.i4_vectors):
                index.add_items(vec, i)
            index.set_ef(50)
            self._hnsw_index = index
        except ImportError:
            pass  # hnswlib not available, skip semantic layer

    # ── Stats ───────────────────────────────────────────────────

    @property
    def size(self) -> int:
        return self._insert_count

    def stats(self) -> Dict[str, Any]:
        return {
            "total_entries": self._insert_count,
            "i1_questions": len(self.i1_by_qid),
            "i2_keys": len(self.i2_by_db_col),
            "i3_keys": len(self.i3_by_db_err),
            "i4_semantic": len(self.i4_entries),
        }
