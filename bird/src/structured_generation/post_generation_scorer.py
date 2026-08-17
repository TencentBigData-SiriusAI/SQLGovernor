"""Generic post-generation scoring features for structured SQL candidates."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

import sqlglot
from sqlglot import exp

from .types import PathPlan, StructuredCandidate, StructuredPlan


_RATIO_TOKENS = ("percent", "percentage", "%", "ratio", "proportion", "rate")
_RANK_TOKENS = ("highest", "lowest", "top", "most", "least", "first", "last", "rank")
_COUNT_TOKENS = ("how many", "number of", "count")
_AVG_TOKENS = ("average", "avg", "mean")
_SUM_TOKENS = ("sum", "total")
_DISTINCT_TOKENS = ("distinct", "different", "unique")
_COMPARE_TOKENS = ("difference", "decrease", "increase", "faster", "slower", "than")
_RANGE_TOKENS = ("between", "from", "to", "before", "after", "below", "above", "over", "under")


@dataclass(slots=True)
class PostGenerationScore:
    score: float
    features: dict[str, Any]


def score_candidate_against_plan(
    *,
    plan: StructuredPlan,
    path: PathPlan | None,
    candidate: StructuredCandidate,
) -> PostGenerationScore:
    sql_text = candidate.sql or ""
    if not sql_text.strip():
        return PostGenerationScore(
            score=0.0,
            features={
                "parse_ok": False,
                "parse_error": "EMPTY_SQL",
                "slot_count_alignment": 0.0,
                "slot_type_alignment": 0.0,
                "anchor_coverage": 0.0,
                "scope_alignment": 0.0,
                "minimality_score": 0.0,
                "spec_issue_penalty": 1.0,
                "semantic_issue_penalty": 0.0,
                "post_generation_score": 0.0,
            },
        )

    try:
        tree = sqlglot.parse_one(sql_text, read="sqlite")
        parse_error = None
    except Exception as exc:
        tree = None
        parse_error = str(exc)

    question_text = f"{plan.question} {plan.evidence}".strip().lower()
    features: dict[str, Any] = {
        "parse_ok": tree is not None,
        "parse_error": parse_error,
        "path_kind": candidate.path_kind,
        "render_score": float(candidate.render_score or 0.0),
        "extra_table_count": len(candidate.introduced_extra_tables),
        "anti_pattern_count": len(candidate.anti_pattern_flags),
        "spec_issue_count": len(candidate.spec_issues),
        "semantic_issue_count": len(candidate.semantic_review_issues),
    }

    if tree is None:
        score = max(
            0.0,
            0.2
            - 0.05 * len(candidate.spec_issues)
            - 0.03 * len(candidate.semantic_review_issues),
        )
        features.update(
            {
                "slot_count_alignment": 0.0,
                "slot_type_alignment": 0.0,
                "anchor_coverage": 0.0,
                "scope_alignment": 0.0,
                "minimality_score": 0.0,
                "post_generation_score": score,
            }
        )
        return PostGenerationScore(score=score, features=features)

    select_exprs = list(tree.selects)
    select_types = [_infer_select_type(expr) for expr in select_exprs]
    tables = {table.name for table in tree.find_all(exp.Table) if table.name}
    joins = list(tree.find_all(exp.Join))
    ctes = list(tree.find_all(exp.CTE))
    subqueries = list(tree.find_all(exp.Subquery))

    slot_count_alignment = _slot_count_alignment(plan, len(select_exprs))
    slot_type_alignment = _slot_type_alignment(plan, select_types)
    anchor_coverage, anchor_literals = _anchor_coverage(question_text, sql_text)
    scope_alignment, scope_features = _scope_alignment(question_text, tree)
    minimality_score = _minimality_score(
        tree=tree,
        tables=tables,
        joins=joins,
        ctes=ctes,
        subqueries=subqueries,
        path=path,
        candidate=candidate,
    )
    spec_issue_penalty = min(1.0, 0.15 * len(candidate.spec_issues))
    semantic_issue_penalty = min(1.0, 0.10 * len(candidate.semantic_review_issues))

    score = (
        0.22 * slot_count_alignment
        + 0.24 * slot_type_alignment
        + 0.22 * anchor_coverage
        + 0.18 * scope_alignment
        + 0.14 * minimality_score
    )
    score -= 0.10 * spec_issue_penalty
    score -= 0.05 * semantic_issue_penalty
    score = max(0.0, min(1.0, score))

    features.update(
        {
            "select_count": len(select_exprs),
            "select_types": select_types,
            "slot_count_alignment": round(slot_count_alignment, 4),
            "slot_type_alignment": round(slot_type_alignment, 4),
            "anchor_coverage": round(anchor_coverage, 4),
            "anchor_literals": sorted(anchor_literals),
            "scope_alignment": round(scope_alignment, 4),
            "minimality_score": round(minimality_score, 4),
            "spec_issue_penalty": round(spec_issue_penalty, 4),
            "semantic_issue_penalty": round(semantic_issue_penalty, 4),
            "table_count": len(tables),
            "join_count": len(joins),
            "cte_count": len(ctes),
            "subquery_count": len(subqueries),
            "scope_features": scope_features,
            "post_generation_score": round(score, 4),
        }
    )
    return PostGenerationScore(score=score, features=features)


def _slot_count_alignment(plan: StructuredPlan, select_count: int) -> float:
    expected = plan.answer_intent.return_slot_count if plan.answer_intent else None
    if expected is None or expected <= 0:
        expected = len(plan.output_spec.columns)
    if expected <= 0:
        return 1.0
    diff = abs(expected - select_count)
    return max(0.0, 1.0 - 0.5 * diff)


def _slot_type_alignment(plan: StructuredPlan, select_types: list[str]) -> float:
    intent = plan.answer_intent
    if not intent or not intent.return_slots:
        return 1.0
    expected = [_normalize_value_type(slot.value_type) for slot in intent.return_slots]
    if not expected:
        return 1.0
    if not select_types:
        return 0.0
    matches = 0
    for index, expected_type in enumerate(expected):
        if index < len(select_types) and expected_type == _normalize_value_type(select_types[index]):
            matches += 1
    ordered_score = matches / max(len(expected), len(select_types))

    expected_counts = _count_tokens(expected)
    actual_counts = _count_tokens(_normalize_value_type(item) for item in select_types)
    overlap = 0
    total = sum(expected_counts.values()) or 1
    for key, value in expected_counts.items():
        overlap += min(value, actual_counts.get(key, 0))
    set_score = overlap / total
    return max(0.0, min(1.0, 0.6 * ordered_score + 0.4 * set_score))


def _anchor_coverage(question_text: str, sql_text: str) -> tuple[float, set[str]]:
    anchors = _extract_literal_anchors(question_text)
    if not anchors:
        return 1.0, set()
    sql_normalized = _normalize_sql_for_anchor_match(sql_text)
    hit_count = 0
    literal_hits: set[str] = set()
    for variants in anchors:
        if any(_normalize_sql_for_anchor_match(variant) in sql_normalized for variant in variants):
            hit_count += 1
            literal_hits.update(variants)
    return hit_count / len(anchors), literal_hits


def _scope_alignment(question_text: str, tree: exp.Expression) -> tuple[float, dict[str, bool]]:
    features: dict[str, bool] = {}
    checks: list[bool] = []
    sql_text = tree.sql(dialect="sqlite").lower()
    has_order = tree.args.get("order") is not None
    has_limit = tree.args.get("limit") is not None
    has_count = any(isinstance(node, exp.Count) for node in tree.walk())
    has_avg = any(isinstance(node, exp.Avg) for node in tree.walk())
    has_sum = any(isinstance(node, exp.Sum) for node in tree.walk())
    has_minmax = any(isinstance(node, (exp.Min, exp.Max)) for node in tree.walk())
    has_division = any(isinstance(node, exp.Div) for node in tree.walk()) or "/" in sql_text
    has_subtraction = any(isinstance(node, exp.Sub) for node in tree.walk()) or "-" in sql_text
    has_distinct = "distinct" in sql_text
    has_group = tree.args.get("group") is not None
    has_range = any(token in sql_text for token in [" between ", ">=", "<=", ">", "<"])

    if any(token in question_text for token in _COUNT_TOKENS):
        features["count_required"] = has_count
        checks.append(has_count)
    if any(token in question_text for token in _AVG_TOKENS):
        features["avg_required"] = has_avg
        checks.append(has_avg)
    if any(token in question_text for token in _SUM_TOKENS):
        features["sum_required"] = has_sum or has_count
        checks.append(has_sum or has_count)
    if any(token in question_text for token in _RATIO_TOKENS):
        features["ratio_required"] = has_division
        checks.append(has_division)
    if any(token in question_text for token in _COMPARE_TOKENS):
        features["compare_required"] = has_subtraction or has_division
        checks.append(has_subtraction or has_division)
    if any(token in question_text for token in _RANK_TOKENS):
        features["rank_required"] = has_order or has_limit or has_minmax
        checks.append(has_order or has_limit or has_minmax)
    if any(token in question_text for token in _DISTINCT_TOKENS):
        features["distinct_required"] = has_distinct
        checks.append(has_distinct)
    if any(token in question_text for token in _RANGE_TOKENS):
        features["range_required"] = has_range
        checks.append(has_range)
    if "group" in question_text or "each " in question_text:
        features["group_required"] = has_group
        checks.append(has_group)

    if not checks:
        return 1.0, features
    return sum(1 for item in checks if item) / len(checks), features


def _minimality_score(
    *,
    tree: exp.Expression,
    tables: set[str],
    joins: list[exp.Join],
    ctes: list[exp.CTE],
    subqueries: list[exp.Subquery],
    path: PathPlan | None,
    candidate: StructuredCandidate,
) -> float:
    penalty = 0.0
    penalty += 0.10 * len(candidate.introduced_extra_tables)
    penalty += 0.05 * len(candidate.anti_pattern_flags)
    penalty += 0.05 * max(0, len(ctes) - 1)
    penalty += 0.04 * len(subqueries)
    if path and path.path_kind != "freeform":
        table_surplus = max(0, len(tables) - len(path.tables))
        join_surplus = max(0, len(joins) - max(0, len(path.tables) - 1))
        penalty += 0.08 * table_surplus
        penalty += 0.04 * join_surplus
    return max(0.0, 1.0 - penalty)


def _infer_select_type(expr: exp.Expression) -> str:
    alias = (expr.alias_or_name or "").lower()
    sql_text = expr.sql(dialect="sqlite").lower()
    if "count(" in sql_text:
        return "count"
    if any(token in alias for token in ("percent", "percentage", "ratio", "rate")) or "/" in sql_text:
        return "percentage"
    if any(token in sql_text for token in ("sum(", "avg(", "min(", "max(", "-", "+", "*")):
        return "metric"
    if "||" in sql_text:
        return "name"
    column_name = _last_column_name(expr)
    return _normalize_value_type(column_name)


def _last_column_name(expr: exp.Expression) -> str:
    if isinstance(expr, exp.Column):
        return expr.name.lower()
    for column in expr.find_all(exp.Column):
        if column.name:
            return column.name.lower()
    alias = expr.alias_or_name
    return (alias or expr.sql(dialect="sqlite")).lower()


def _normalize_value_type(value: str | None) -> str:
    lowered = (value or "").lower()
    if any(token in lowered for token in ("percent", "percentage", "ratio", "rate")):
        return "percentage"
    if "count" in lowered:
        return "count"
    if "date" in lowered or "time" in lowered or "year" in lowered or "birthday" in lowered or "dob" in lowered:
        return "date"
    if "code" in lowered:
        return "code"
    if lowered.endswith("id") or "_id" in lowered or " id" in lowered:
        return "id"
    if "name" in lowered or "fname" in lowered or "lname" in lowered or "title" in lowered:
        return "name"
    if "type" in lowered or "description" in lowered or "desc" in lowered:
        return "description"
    if lowered:
        return "metric"
    return "unknown"


def _extract_literal_anchors(text: str) -> list[set[str]]:
    anchors: list[set[str]] = []
    seen: set[tuple[str, ...]] = set()

    for token in re.findall(r"\b\d{1,4}[/-]\d{1,2}[/-]\d{1,4}\b", text):
        variants = _date_variants(token)
        key = tuple(sorted(variants))
        if key not in seen:
            seen.add(key)
            anchors.append(variants)

    for token in re.findall(r"(?<![A-Za-z_])\d+(?:\.\d+)?(?![A-Za-z_])", text):
        variants = {token}
        if "." in token:
            variants.add(str(float(token)))
        key = tuple(sorted(variants))
        if key not in seen:
            seen.add(key)
            anchors.append(variants)
    return anchors


def _date_variants(token: str) -> set[str]:
    parts = re.split(r"[/-]", token)
    variants = {token}
    if len(parts) != 3:
        return variants
    if len(parts[0]) == 4:
        year, month, day = parts
    elif len(parts[2]) == 4:
        month, day, year = parts
    else:
        return variants
    try:
        variants.add(f"{int(year):04d}-{int(month):02d}-{int(day):02d}")
        variants.add(f"{int(year):04d}/{int(month):02d}/{int(day):02d}")
        variants.add(f"{int(year):04d}-{int(month)}-{int(day)}")
        variants.add(f"{int(year):04d}/{int(month)}/{int(day)}")
    except ValueError:
        pass
    return variants


def _normalize_sql_for_anchor_match(text: str) -> str:
    return re.sub(r"['\"`\\s]+", "", text.lower())


def _count_tokens(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts
