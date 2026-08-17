"""Hierarchical retrieval with specificity-weighted scoring.

Score(e, q_new) = Σ_ℓ  w_ℓ · 𝟙[e ∈ R_ℓ] · Sim_ℓ(e, q_new) · Conf(e)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from error_bank.schema import ErrorEntry
from error_bank.store import ErrorBankStore
from error_bank.confidence import THRESHOLD_WARN, THRESHOLD_HINT, THRESHOLD_EXAMPLE


# Specificity decay: w_ℓ = α^(ℓ-1)
ALPHA = 0.6
WEIGHTS = {1: 1.0, 2: ALPHA, 3: ALPHA ** 2, 4: ALPHA ** 3}


@dataclass
class RetrievalResult:
    entry: ErrorEntry
    score: float
    layer: int        # 1-4
    layer_name: str   # "instance", "column", "schema", "semantic"
    confidence: float


def retrieve(
    store: ErrorBankStore,
    question_id: int,
    db_id: str,
    tables: List[str],
    columns: List[str],
    question_text: str = "",
    k: int = 10,
) -> List[RetrievalResult]:
    """Hierarchical retrieval across all 4 layers.

    Returns results sorted by score (highest first).
    Each result tagged with its layer and confidence.
    """
    results: List[RetrievalResult] = []
    seen_entry_ids = set()  # avoid duplicates across layers

    def _entry_key(e: ErrorEntry) -> str:
        return f"{e.question_id}:{e.error_phase}:{e.error_type}:{e.sql_text[:50]}"

    # ── Layer 1: Instance (same question_id) ──
    l1 = store.query_by_qid(question_id)
    for e in l1:
        eid = _entry_key(e)
        if eid in seen_entry_ids:
            continue
        seen_entry_ids.add(eid)

        conf = store.confidence.get_confidence(
            e.db_id, e.schema_anchor_key, e.error_type.value
        )
        score = WEIGHTS[1] * 1.0 * conf  # Sim_1 = 1.0 (exact match)
        results.append(RetrievalResult(
            entry=e, score=score, layer=1, layer_name="instance", confidence=conf
        ))

    # ── Layer 2: Column-level (same db + same table.column) ──
    for col in columns:
        l2 = store.query_by_db_column(db_id, col)
        for e in l2:
            if e.question_id == question_id:
                continue  # already in L1
            eid = _entry_key(e)
            if eid in seen_entry_ids:
                continue
            seen_entry_ids.add(eid)

            conf = store.confidence.get_confidence(db_id, col, e.error_type.value)
            # Sim_2 = column overlap
            sim = _column_overlap(e.columns + e.tables, columns + tables)
            score = WEIGHTS[2] * sim * conf
            results.append(RetrievalResult(
                entry=e, score=score, layer=2, layer_name="column", confidence=conf
            ))

    for table in tables:
        l2t = store.query_by_db_table(db_id, table)
        for e in l2t:
            if e.question_id == question_id:
                continue
            eid = _entry_key(e)
            if eid in seen_entry_ids:
                continue
            seen_entry_ids.add(eid)

            conf = store.confidence.get_confidence(db_id, table, e.error_type.value)
            sim = _column_overlap(e.columns + e.tables, columns + tables)
            score = WEIGHTS[2] * sim * conf
            results.append(RetrievalResult(
                entry=e, score=score, layer=2, layer_name="column", confidence=conf
            ))

    # ── Layer 3: Schema-level (same db + same error type) ──
    seen_types = set()
    for e_existing in results:
        seen_types.add(e_existing.entry.error_type.value)

    for etype_val in set(e.error_type.value for e in store.i3_by_db_err.get((db_id,), [])) | seen_types:
        l3 = store.query_by_db_error_type(db_id, etype_val)
        for e in l3:
            eid = _entry_key(e)
            if eid in seen_entry_ids:
                continue
            seen_entry_ids.add(eid)

            conf = store.confidence.get_confidence(db_id, e.schema_anchor_key, e.error_type.value)
            sim = _schema_jaccard(e.tables, tables)
            score = WEIGHTS[3] * sim * conf
            if score > 0.15:  # skip low scores — Layer 3 is noisy
                results.append(RetrievalResult(
                    entry=e, score=score, layer=3, layer_name="schema", confidence=conf
                ))

    # ── Layer 4: Semantic (HNSW) ──
    if question_text:
        l4 = store.query_semantic(question_text, db_id, k=k)
        for e, dist in l4:
            eid = _entry_key(e)
            if eid in seen_entry_ids:
                continue
            seen_entry_ids.add(eid)

            conf = store.confidence.get_confidence(db_id, e.schema_anchor_key, e.error_type.value)
            sim = max(0, 1.0 - dist)
            score = WEIGHTS[4] * sim * conf
            if score > 0.05:
                results.append(RetrievalResult(
                    entry=e, score=score, layer=4, layer_name="semantic", confidence=conf
                ))

    # Sort by score descending, take top-K
    results.sort(key=lambda r: -r.score)
    return results[:k]


def _column_overlap(cols_a: List[str], cols_b: List[str]) -> float:
    """Weighted Jaccard similarity: columns weight 1.0, tables weight 0.5."""
    if not cols_a and not cols_b:
        return 0.0

    def weight(name: str) -> float:
        return 1.0 if "." in name else 0.5  # "table.column" vs "table"

    set_a = set(c.lower() for c in cols_a)
    set_b = set(c.lower() for c in cols_b)
    intersection = set_a & set_b
    union = set_a | set_b

    if not union:
        return 0.0

    num = sum(weight(c) for c in intersection)
    den = sum(weight(c) for c in union)
    return num / den if den > 0 else 0.0


def _schema_jaccard(tables_a: List[str], tables_b: List[str]) -> float:
    """Simple Jaccard on table sets."""
    a = set(t.lower() for t in tables_a)
    b = set(t.lower() for t in tables_b)
    if not a and not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0
