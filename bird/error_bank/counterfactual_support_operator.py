from __future__ import annotations

from difflib import SequenceMatcher
import re
import sqlite3
from typing import Any

from sqlglot import exp, parse_one


TARGET_ROOT_CAUSES = {
    "FUZZY_MISMATCH",
    "CASE_MISMATCH",
    "PREFIX_MATCH",
    "SUBSTRING_MATCH",
    "VALUE_NOT_EXISTS",
    "EXACT_MATCH_EXISTS",
    "COMPLEX_WHERE_KILLER",
}


def suggest_counterfactual_support_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    db_path: str,
    max_candidates: int = 3,
) -> list[dict[str, Any]]:
    failure_spec = structured_diagnosis.get("failure_spec") or {}
    root_cause = str(failure_spec.get("root_cause_type") or "UNKNOWN")
    if root_cause not in TARGET_ROOT_CAUSES:
        return []

    query_spec = structured_diagnosis.get("query_spec") or {}
    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    killer_expr = _find_killer_expression(query_spec, failure_spec)
    if not killer_expr:
        return []
    parsed = _parse_simple_literal_predicate(killer_expr)
    if not parsed:
        return []
    target_column, operator, literal = parsed

    support_values = _collect_support_values(
        sql_text=sql_text,
        killer_expr=killer_expr,
        target_column=target_column,
        db_path=db_path,
    )
    if not support_values:
        return []

    suggestions = []
    if operator == "eq":
        candidates = _candidate_replacements_for_eq(literal, support_values)
        for replacement, rationale in candidates[:max_candidates]:
            new_sql = _replace_predicate(sql_text, killer_expr, f"{target_column} = '{replacement}'")
            if new_sql and new_sql != sql_text:
                suggestions.append(
                    {
                        "sql": new_sql,
                        "operator": "counterfactual_support",
                        "rule": rationale,
                        "original_predicate": killer_expr,
                        "support_values": support_values[:8],
                    }
                )
    elif operator == "like":
        patterns = _candidate_patterns_for_like(literal, support_values)
        for pattern, rationale in patterns[:max_candidates]:
            new_sql = _replace_predicate(sql_text, killer_expr, f"{target_column} LIKE '{pattern}'")
            if new_sql and new_sql != sql_text:
                suggestions.append(
                    {
                        "sql": new_sql,
                        "operator": "counterfactual_support",
                        "rule": rationale,
                        "original_predicate": killer_expr,
                        "support_values": support_values[:8],
                    }
                )

    deduped = []
    seen = set()
    for item in suggestions:
        sql = item["sql"]
        if sql in seen:
            continue
        seen.add(sql)
        deduped.append(item)
    return deduped[:max_candidates]


def _find_killer_expression(query_spec: dict[str, Any], failure_spec: dict[str, Any]) -> str:
    killer = _normalize_predicate_text(str(failure_spec.get("killer_unit") or ""))
    for pred in query_spec.get("where_predicates") or []:
        expr = str(pred.get("expression") or "")
        if _normalize_predicate_text(expr) == killer or _normalize_predicate_text(expr) in killer or killer in _normalize_predicate_text(expr):
            return expr
    return ""


def _parse_simple_literal_predicate(expr: str) -> tuple[str, str, str] | None:
    m = re.match(r"^\s*(?P<lhs>.+?)\s*=\s*'(?P<lit>[^']+)'\s*$", expr, re.I)
    if m:
        return m.group("lhs").strip(), "eq", m.group("lit")
    m = re.match(r"^\s*(?P<lhs>.+?)\s+LIKE\s+'(?P<lit>[^']+)'\s*$", expr, re.I)
    if m:
        return m.group("lhs").strip(), "like", m.group("lit")
    return None


def _collect_support_values(*, sql_text: str, killer_expr: str, target_column: str, db_path: str) -> list[str]:
    try:
        tree = parse_one(sql_text, read="sqlite")
    except Exception:
        return []
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return []

    # Remove the killer predicate from the WHERE clause.
    if select.args.get("where") is None or select.args["where"].this is None:
        return []
    normalized_killer = _normalize_predicate_text(killer_expr)
    new_where = _remove_predicate(select.args["where"].this, normalized_killer)
    if new_where is None:
        select.set("where", None)
    else:
        select.set("where", exp.Where(this=new_where))

    # Project only the target column to inspect surviving values.
    select.set("expressions", [parse_one(f"SELECT {target_column}", read="sqlite").expressions[0]])
    select.set("distinct", exp.Distinct())
    select.set("order", None)
    select.set("limit", exp.Limit(expression=exp.Literal.number(20)))
    select.set("group", None)
    select.set("having", None)

    support_sql = tree.sql(dialect="sqlite")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    try:
        rows = conn.execute(support_sql).fetchall()
    except Exception:
        return []
    finally:
        conn.close()
    values = []
    for row in rows:
        if not row:
            continue
        val = row[0]
        if val is None:
            continue
        values.append(str(val))
    return values


def _remove_predicate(node: exp.Expression, normalized_target: str) -> exp.Expression | None:
    if isinstance(node, exp.And):
        left = _remove_predicate(node.this, normalized_target)
        right = _remove_predicate(node.expression, normalized_target)
        if left is None:
            return right
        if right is None:
            return left
        return exp.and_(left, right)
    if _normalize_predicate_text(node.sql(dialect="sqlite")) == normalized_target:
        return None
    return node


def _candidate_replacements_for_eq(literal: str, support_values: list[str]) -> list[tuple[str, str]]:
    out = []
    for value in support_values:
        if value == literal:
            continue
        if literal.lower() == value.lower():
            out.append((value, "support_exact_case_normalization"))
            continue
        if _is_date_prefix_match(literal, value):
            out.append((value, "support_date_format_normalization"))
            continue
        sim = SequenceMatcher(None, _normalize_text(literal), _normalize_text(value)).ratio()
        if sim >= 0.8:
            out.append((value, "support_similar_value_substitution"))
    return out


def _candidate_patterns_for_like(literal: str, support_values: list[str]) -> list[tuple[str, str]]:
    out = []
    if re.fullmatch(r"\d{1,2}:\d{2}:\d{2}", literal):
        parts = literal.split(":")
        minute = str(int(parts[1]))
        second = parts[2]
        prefix = f"{minute}:{second}%"
        if any(f"{minute}:{second}" in val for val in support_values):
            out.append((prefix, "support_time_prefix_normalization"))
            out.append((f"%{minute}:{second}%", "support_time_contains_normalization"))
    if literal.endswith("%"):
        bare = literal.rstrip("%")
        if re.fullmatch(r"\d{6}", bare):
            out.append((f"{bare[:4]}-{bare[4:]}%", "support_month_dash_normalization"))
    return out


def _replace_predicate(sql_text: str, original_predicate: str, replacement_predicate: str) -> str:
    try:
        tree = parse_one(sql_text, read="sqlite")
        wrapper = parse_one(f"SELECT * FROM __tmp__ WHERE {replacement_predicate}", read="sqlite")
    except Exception:
        return ""
    where_expr = wrapper.args.get("where")
    if where_expr is None or where_expr.this is None:
        return ""
    replacement_node = where_expr.this
    target_norm = _normalize_predicate_text(original_predicate)
    replaced = {"count": 0}

    def _transform(node: exp.Expression) -> exp.Expression:
        if _normalize_predicate_text(node.sql(dialect="sqlite")) == target_norm:
            replaced["count"] += 1
            return replacement_node.copy()
        return node

    updated = tree.transform(_transform)
    if replaced["count"] <= 0:
        return ""
    return updated.sql(dialect="sqlite")


def _normalize_predicate_text(text: str) -> str:
    text = re.sub(r"^\[(?:!SOLO_KILL|L|R)\]\s*", "", str(text or ""), flags=re.I)
    return " ".join(text.strip().lower().split())


def _normalize_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _is_date_prefix_match(left: str, right: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", left) and right.startswith(left))
