from __future__ import annotations

import re
from typing import Any


def suggest_where_predicate_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    max_candidates: int = 4,
) -> list[dict[str, Any]]:
    patch_plan = structured_diagnosis.get("patch_plan") or {}
    if patch_plan.get("patch_goal") != "fix_where_predicate":
        return []

    failure_spec = structured_diagnosis.get("failure_spec") or {}
    if str(failure_spec.get("root_cause_type") or "") != "COMPLEX_WHERE_KILLER":
        return []

    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    query_spec = structured_diagnosis.get("query_spec") or {}
    killer = _normalize_predicate_text(str(failure_spec.get("killer_unit") or ""))
    predicates = query_spec.get("where_predicates") or []

    target_predicate = ""
    for item in predicates:
        expr = _normalize_predicate_text(str(item.get("expression") or ""))
        if expr == killer or expr in killer or killer in expr:
            target_predicate = str(item.get("expression") or "")
            break
    if not target_predicate:
        target_predicate = killer

    suggestions: list[dict[str, Any]] = []
    for item in predicates:
        expr = str(item.get("expression") or "")
        if _normalize_predicate_text(expr) != _normalize_predicate_text(target_predicate):
            continue

        like_variants = _build_like_normalizations(expr)
        for new_predicate, rationale in like_variants:
            new_sql = sql_text.replace(expr, new_predicate, 1)
            if new_sql != sql_text:
                suggestions.append(
                    {
                        "sql": new_sql,
                        "operator": "where_predicate",
                        "rule": rationale,
                        "original_predicate": expr,
                        "replacement_predicate": new_predicate,
                    }
                )

        subquery_variant = _build_scalar_subquery_in_variant(expr)
        if subquery_variant:
            new_predicate, rationale = subquery_variant
            new_sql = sql_text.replace(expr, new_predicate, 1)
            if new_sql != sql_text:
                suggestions.append(
                    {
                        "sql": new_sql,
                        "operator": "where_predicate",
                        "rule": rationale,
                        "original_predicate": expr,
                        "replacement_predicate": new_predicate,
                    }
                )

    deduped: list[dict[str, Any]] = []
    seen_sql: set[str] = set()
    for item in suggestions:
        sql = item["sql"]
        if sql in seen_sql:
            continue
        seen_sql.add(sql)
        deduped.append(item)
        if len(deduped) >= max_candidates:
            break
    return deduped


def _build_like_normalizations(predicate: str) -> list[tuple[str, str]]:
    match = re.search(r"^(?P<lhs>.+?)\s+LIKE\s+'(?P<pattern>[^']+)'$", predicate, re.I)
    if not match:
        return []

    lhs = match.group("lhs").strip()
    pattern = match.group("pattern").strip()
    variants: list[tuple[str, str]] = []

    time_prefix = _normalize_time_like_pattern(pattern)
    if time_prefix:
        variants.append((f"{lhs} LIKE '{time_prefix}'", "time_like_prefix_normalization"))
        variants.append((f"{lhs} LIKE '%{time_prefix[:-1]}%'", "time_like_contains_normalization"))

    month_key = _normalize_month_key(pattern)
    if month_key:
        variants.append(
            (
                f"SUBSTR(REPLACE(CAST({lhs} AS TEXT), '-', ''), 1, 6) = '{month_key}'",
                "date_month_format_normalization",
            )
        )

    if "%" not in pattern and "_" not in pattern:
        variants.append((f"{lhs} LIKE '{pattern}%'", "like_prefix_normalization"))

    return variants


def _build_scalar_subquery_in_variant(predicate: str) -> tuple[str, str] | None:
    if "SELECT" not in predicate.upper():
        return None
    match = re.search(r"^(?P<lhs>.+?)\s*=\s*(?P<rhs>\(\s*SELECT.+\))$", predicate, re.I | re.S)
    if not match:
        return None
    lhs = match.group("lhs").strip()
    rhs = match.group("rhs").strip()
    return (f"{lhs} IN {rhs}", "scalar_subquery_to_in")


def _normalize_time_like_pattern(pattern: str) -> str | None:
    cleaned = pattern.strip()
    if "%" in cleaned or "_" in cleaned:
        return None
    if not re.fullmatch(r"\d{1,2}:\d{2}:\d{2}", cleaned):
        return None
    parts = cleaned.split(":")
    if len(parts) != 3:
        return None
    minutes = str(int(parts[1]))
    seconds = parts[2]
    return f"{minutes}:{seconds}%"


def _normalize_month_key(pattern: str) -> str | None:
    cleaned = pattern.strip().rstrip("%")
    if re.fullmatch(r"\d{6}", cleaned):
        return cleaned
    if re.fullmatch(r"\d{4}-\d{2}", cleaned):
        return cleaned.replace("-", "")
    return None


def _normalize_predicate_text(text: str) -> str:
    text = text or ""
    text = re.sub(r"^\[(?:!SOLO_KILL|L|R)\]\s*", "", text)
    return " ".join(text.split()).strip().lower()
