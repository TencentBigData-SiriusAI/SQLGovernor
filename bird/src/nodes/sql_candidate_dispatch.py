"""Dispatch the next candidate from the queue for validation."""

from __future__ import annotations

from typing import Any, Dict, List

from loguru import logger

from ..core.state import SQLAgentState


def sql_candidate_dispatch_node(state: SQLAgentState) -> Dict[str, Any]:
    """Pop next candidate from the queue and prepare it for validation."""

    candidates: List[Dict[str, Any]] = state.get("generation_candidates", [])
    queue: List[int] = list(state.get("candidate_queue", []))

    if not queue:
        logger.warning("Candidate queue is empty")
        return {
            "candidate_queue": queue,
            "candidate_sql": "",
            "current_candidate_index": None,
            "current_candidate_meta": {},
            "current_attempt_index": None,
            "candidate_available": False,
        }

    next_local_idx = queue.pop(0)
    candidate: Dict[str, Any]
    if 0 <= next_local_idx < len(candidates):
        candidate = candidates[next_local_idx]
    else:
        candidate = {}
    sql_text = candidate.get("sql", "")

    logger.debug(
        f"Dispatching candidate #{next_local_idx}, remaining {len(queue)}"
    )

    candidate_meta = {
        "index": candidate.get("index", next_local_idx),
        "queue_index": next_local_idx,
        "vote_count": candidate.get("vote_count", 0),
        "source": candidate.get("source", "generation"),
    }

    return {
        "candidate_queue": queue,
        "candidate_sql": sql_text,
        "current_candidate_index": next_local_idx,
        "current_candidate_meta": candidate_meta,
        "current_attempt_index": None,
        "candidate_available": True,
        "validation_result": {},
        "execution_result": {},
    }
