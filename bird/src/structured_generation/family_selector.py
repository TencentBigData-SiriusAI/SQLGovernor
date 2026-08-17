"""Family identifiers and minimal family-aware selection utilities."""

from __future__ import annotations

from collections import defaultdict
import hashlib
import json
from typing import Any

from .types import PathPlan, StructuredCandidate


def assign_family_id(path: PathPlan) -> str:
    signature = {
        "path_kind": path.path_kind,
        "tables": sorted(path.tables),
        "join_edges": sorted(path.join_edges),
        "bridge_tables": sorted(path.bridge_tables),
        "key_family_choices": path.key_family_choices,
        "slot_strategy": path.slot_strategy,
        "owner_decisions": path.owner_decisions,
    }
    digest = hashlib.sha1(
        json.dumps(signature, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"fam_{digest[:10]}"


def collapse_candidates_by_family(
    candidates: list[StructuredCandidate],
    *,
    keep_per_family: int = 1,
) -> list[StructuredCandidate]:
    grouped: dict[str, list[StructuredCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.family_id].append(candidate)

    kept: list[StructuredCandidate] = []
    for family_id in sorted(grouped):
        family_candidates = sorted(grouped[family_id], key=_family_sort_key, reverse=True)
        kept.extend(family_candidates[:keep_per_family])
    return kept


def select_family_representatives(
    candidates: list[StructuredCandidate],
    *,
    keep_per_family: int = 1,
) -> tuple[list[StructuredCandidate], dict[str, dict[str, Any]]]:
    grouped: dict[str, list[StructuredCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.family_id].append(candidate)

    representatives: list[StructuredCandidate] = []
    family_summaries: dict[str, dict[str, Any]] = {}
    for family_id in sorted(grouped):
        family_candidates = grouped[family_id]
        successful = [item for item in family_candidates if item.execution.get("success")]
        chosen: list[StructuredCandidate]
        if successful:
            clusters: dict[Any, list[StructuredCandidate]] = defaultdict(list)
            for candidate in successful:
                clusters[candidate.execution.get("rows_signature")].append(candidate)
            best_cluster = max(clusters.values(), key=_family_cluster_key)
            chosen = sorted(best_cluster, key=_executed_candidate_key, reverse=True)[:keep_per_family]
            family_summaries[family_id] = {
                "family_size": len(family_candidates),
                "success_count": len(successful),
                "result_cluster_count": len(clusters),
                "selected_candidate_ids": [item.candidate_id for item in chosen],
                "selection_reason": "executed_family_majority",
            }
        else:
            chosen = sorted(family_candidates, key=_family_sort_key, reverse=True)[:keep_per_family]
            family_summaries[family_id] = {
                "family_size": len(family_candidates),
                "success_count": 0,
                "result_cluster_count": 0,
                "selected_candidate_ids": [item.candidate_id for item in chosen],
                "selection_reason": "best_non_successful_fallback",
            }
        representatives.extend(chosen)
    return representatives, family_summaries


def select_winning_candidate(candidates: list[StructuredCandidate]) -> dict[str, Any]:
    successful = [candidate for candidate in candidates if candidate.execution.get("success")]
    if not successful:
        return {
            "winner_candidate_id": None,
            "winner_family_id": None,
            "winning_reason": "no_successful_candidates",
            "cluster_count": 0,
        }

    clusters: dict[Any, list[StructuredCandidate]] = defaultdict(list)
    for candidate in successful:
        clusters[candidate.execution.get("rows_signature")].append(candidate)

    def _cluster_key(items: list[StructuredCandidate]) -> tuple[int, int, float]:
        family_count = len({item.family_id for item in items})
        candidate_count = len(items)
        best_render_score = max(item.render_score for item in items)
        return (family_count, candidate_count, best_render_score)

    cluster_members = max(clusters.values(), key=_cluster_key)
    winner = max(cluster_members, key=lambda item: item.render_score)
    return {
        "winner_candidate_id": winner.candidate_id,
        "winner_family_id": winner.family_id,
        "winning_reason": "family_aware_result_majority",
        "cluster_count": len(clusters),
        "support_family_count": len({item.family_id for item in cluster_members}),
        "support_candidate_count": len(cluster_members),
    }


def _family_sort_key(candidate: StructuredCandidate) -> tuple[int, float, int]:
    is_valid = 1 if candidate.is_valid else 0
    render_score = float(candidate.render_score or 0.0)
    error_count = -len(candidate.errors)
    return (
        is_valid,
        render_score,
        candidate.execution.get("success", False),
        -candidate.auto_correct_attempts,
        error_count,
    )


def _family_cluster_key(items: list[StructuredCandidate]) -> tuple[int, float, int]:
    family_score = max(item.render_score for item in items)
    best_attempts = min(item.auto_correct_attempts for item in items)
    return (len(items), family_score, -best_attempts)


def _executed_candidate_key(candidate: StructuredCandidate) -> tuple[int, float, float, int]:
    return (
        1 if candidate.execution.get("success") else 0,
        float(candidate.render_score or 0.0),
        -float(candidate.execution.get("elapsed") or 0.0),
        -candidate.auto_correct_attempts,
    )
