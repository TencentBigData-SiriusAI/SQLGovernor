"""Deterministic structural-support gate for low-confidence selection ties."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from error_bank.diagnosis_spec import QueryPredicate, _build_query_spec
from scripts.experiment_qwen3_evidence_synth_selector import summarize_query_logic
from scripts.tune_selection_score_weights import normalize_sql


SIGNATURE_FIELDS = (
    "table_set",
    "projection_kinds",
    "projected_column_count",
    "has_distinct",
    "has_count_distinct",
    "has_group_by",
    "group_by_count",
    "has_order_by",
    "order_dirs",
    "limit_bucket",
    "projected_columns",
    "predicate_columns",
)


def _normalize_text(text: Any) -> str:
    value = str(text or "").lower()
    value = re.sub(r"[`\"\[\]]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _projection_kind(expression: str) -> str:
    expr = (expression or "").upper()
    if "COUNT(DISTINCT" in expr:
        return "COUNT_DISTINCT"
    if "COUNT(" in expr:
        return "COUNT"
    if "SUM(" in expr:
        return "SUM"
    if "AVG(" in expr:
        return "AVG"
    if "MIN(" in expr:
        return "MIN"
    if "MAX(" in expr:
        return "MAX"
    if "CONCAT(" in expr or "||" in expr:
        return "CONCAT"
    if "CASE " in expr:
        return "CASE"
    return "PLAIN"


def _order_dir(expression: str) -> str:
    expr = (expression or "").upper()
    if re.search(r"\bDESC\b", expr):
        return "DESC"
    if re.search(r"\bASC\b", expr):
        return "ASC"
    return "UNSPECIFIED"


def _predicate_shape(predicate: QueryPredicate) -> str:
    columns = "|".join(sorted(_normalize_text(col) for col in (predicate.columns or [])))
    operator = _normalize_text(predicate.operator)
    literal_types: list[str] = []
    for literal in predicate.literals or []:
        text = str(literal or "").strip()
        if re.fullmatch(r"-?\d+(\.\d+)?", text):
            literal_types.append("NUM")
        elif text.startswith(("'", '"')) and text.endswith(("'", '"')):
            literal_types.append("STR")
        else:
            literal_types.append("OTHER")
    literal_bucket = "|".join(sorted(literal_types))
    return f"{columns}::{operator}::{literal_bucket}"


def extract_structure_features(sql_text: str) -> dict[str, Any]:
    query_spec = _build_query_spec(sql_text)
    logic = summarize_query_logic(query_spec)
    projections = getattr(query_spec, "projections", []) or []
    where_predicates = getattr(query_spec, "where_predicates", []) or []
    sql_upper = (getattr(query_spec, "sql_text", "") or "").upper()
    return {
        "table_set": tuple(sorted(_normalize_text(table) for table in query_spec.base_tables)),
        "projection_kinds": tuple(_projection_kind(getattr(item, "expression", "")) for item in projections),
        "projected_column_count": int(logic.get("projected_column_count") or 0),
        "has_distinct": "SELECT DISTINCT" in sql_upper,
        "has_count_distinct": "COUNT(DISTINCT" in sql_upper,
        "has_group_by": bool(logic.get("has_group_by")),
        "group_by_count": len(query_spec.group_by),
        "has_order_by": bool(query_spec.order_by),
        "order_dirs": tuple(_order_dir(item) for item in (query_spec.order_by or [])),
        "limit_bucket": 0 if query_spec.limit is None else 1 if int(query_spec.limit) == 1 else 2,
        "projected_columns": tuple(sorted(_normalize_text(col) for col in (logic.get("projected_columns") or []))),
        "predicate_columns": tuple(
            sorted(
                {
                    _normalize_text(column)
                    for pred in where_predicates
                    for column in (pred.columns or [])
                    if _normalize_text(column)
                }
            )
        ),
        "normalized_predicate_shapes": tuple(sorted(_predicate_shape(pred) for pred in where_predicates)),
    }


def build_signature(features: dict[str, Any]) -> tuple[Any, ...]:
    return tuple((field_name, features[field_name]) for field_name in SIGNATURE_FIELDS)


def build_support_stats(candidate_rows: list[dict[str, Any]], signature: tuple[Any, ...]) -> dict[str, int]:
    stats = {
        "candidate_count": 0,
        "vote_sum": 0,
        "unique_sql_count": 0,
        "source_group_count": 0,
        "source_model_count": 0,
    }
    unique_sqls: set[str] = set()
    source_groups: set[str] = set()
    source_models: set[str] = set()
    for row in candidate_rows:
        if row["signature"] != signature:
            continue
        stats["candidate_count"] += 1
        stats["vote_sum"] += int(row.get("vote_count") or 0)
        unique_sqls.add(row["sql"])
        source_group = str(row.get("source_group") or "")
        if source_group:
            source_groups.add(source_group)
        source_model = str(row.get("source_model") or "")
        if source_model:
            source_models.add(source_model)
    stats["unique_sql_count"] = len(unique_sqls)
    stats["source_group_count"] = len(source_groups)
    stats["source_model_count"] = len(source_models)
    return stats


def get_top_merged_groups(row: dict[str, Any]) -> list[int]:
    group_best_scores: dict[int, float] = {}
    for group_id, score in zip(row.get("group_id_list") or [], row.get("merged_score") or []):
        group_id = int(group_id)
        if group_id < 0:
            continue
        group_best_scores[group_id] = max(group_best_scores.get(group_id, float("-inf")), float(score))
    if not group_best_scores:
        return []
    top_score = max(group_best_scores.values())
    return sorted(group_id for group_id, score in group_best_scores.items() if score == top_score)


def choose_group_representatives(
    *,
    row: dict[str, Any],
    candidates: list[dict[str, Any]],
    target_groups: list[int],
) -> dict[int, int]:
    pred_sqls = row.get("pred_sqls") or []
    group_id_list = [int(x) for x in (row.get("group_id_list") or [])]
    merged_scores = [float(x) for x in (row.get("merged_score") or [])]

    best_by_group: dict[int, tuple[tuple[float, int, int], int]] = {}
    target_set = set(target_groups)
    for idx, group_id in enumerate(group_id_list):
        if group_id not in target_set or idx >= len(pred_sqls):
            continue
        vote_count = int((candidates[idx].get("vote_count") or 0) if idx < len(candidates) else 0)
        rank_key = (merged_scores[idx], vote_count, -idx)
        existing = best_by_group.get(group_id)
        if existing is None or rank_key > existing[0]:
            best_by_group[group_id] = (rank_key, idx)
    return {group_id: item[1] for group_id, item in best_by_group.items()}


def build_structural_support_decision(
    *,
    checkpoint_row: dict[str, Any],
    sample: dict[str, Any],
) -> dict[str, Any]:
    qid = int(checkpoint_row["question_id"])
    top_groups = get_top_merged_groups(checkpoint_row)
    pred_sqls = checkpoint_row.get("pred_sqls") or []
    candidates = sample.get("sql_candidates") or []
    representatives = choose_group_representatives(
        row=checkpoint_row,
        candidates=candidates,
        target_groups=top_groups,
    )

    feature_cache: dict[str, dict[str, Any]] = {}

    def _features(sql_text: str) -> dict[str, Any]:
        sql_text = normalize_sql(sql_text)
        if sql_text not in feature_cache:
            feature_cache[sql_text] = extract_structure_features(sql_text)
        return feature_cache[sql_text]

    candidate_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        sql_text = normalize_sql(candidate.get("sql"))
        if not sql_text:
            continue
        candidate_rows.append(
            {
                "sql": sql_text,
                "vote_count": int(candidate.get("vote_count") or 0),
                "source_group": candidate.get("source_group"),
                "source_model": candidate.get("source_model"),
                "signature": build_signature(_features(sql_text)),
            }
        )

    group_details: list[dict[str, Any]] = []
    for group_id in top_groups:
        rep_idx = representatives.get(group_id)
        rep_sql = normalize_sql(pred_sqls[rep_idx]) if rep_idx is not None and rep_idx < len(pred_sqls) else ""
        if not rep_sql:
            continue
        support = build_support_stats(candidate_rows, build_signature(_features(rep_sql)))
        group_details.append(
            {
                "group_id": int(group_id),
                "representative_index": int(rep_idx) if rep_idx is not None else -1,
                "representative_sql": rep_sql,
                "support": support,
            }
        )

    baseline_detail = group_details[0] if group_details else None
    metric_name = "source_group_count"
    if group_details and max(item["support"]["source_group_count"] for item in group_details) == 0:
        metric_name = "source_model_count"

    structural_winner = None
    if group_details:
        structural_winner = max(
            group_details,
            key=lambda item: (
                item["support"][metric_name],
                item["support"]["source_model_count"],
                item["support"]["candidate_count"],
                item["support"]["vote_sum"],
                -item["representative_index"],
            ),
        )

    gate_passed = False
    chosen_sql = baseline_detail["representative_sql"] if baseline_detail else ""
    reason = "no_top_group_tie"
    if len(group_details) <= 1:
        reason = "no_real_top_group_tie"
    elif baseline_detail is None or structural_winner is None:
        reason = "missing_group_representative"
    else:
        baseline_support = baseline_detail["support"][metric_name]
        winner_support = structural_winner["support"][metric_name]
        if (
            structural_winner["group_id"] != baseline_detail["group_id"]
            and winner_support > baseline_support
        ):
            gate_passed = True
            chosen_sql = structural_winner["representative_sql"]
            reason = (
                f"{metric_name}_override:"
                f" baseline={baseline_support} -> challenger={winner_support}"
            )
        else:
            reason = (
                f"keep_baseline:{metric_name}"
                f" baseline={baseline_support}, challenger={winner_support}"
            )

    return {
        "question_id": qid,
        "db_id": sample.get("db_id"),
        "difficulty": sample.get("difficulty", "unknown"),
        "selector_type": "structural_support_gate",
        "signature_fields": list(SIGNATURE_FIELDS),
        "top_group_count": len(group_details),
        "support_metric": metric_name,
        "gate_passed": gate_passed,
        "reason": reason,
        "chosen_sql": chosen_sql if gate_passed else "",
        "baseline_group_id": baseline_detail["group_id"] if baseline_detail else None,
        "baseline_sql": baseline_detail["representative_sql"] if baseline_detail else "",
        "structural_winner_group_id": structural_winner["group_id"] if structural_winner else None,
        "structural_winner_sql": structural_winner["representative_sql"] if structural_winner else "",
        "top_groups": group_details,
    }

