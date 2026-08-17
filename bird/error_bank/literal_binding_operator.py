from __future__ import annotations

from difflib import SequenceMatcher
import re
from typing import Any

from sqlglot import exp, parse_one


_LITERAL_ROOT_CAUSES = {
    "FUZZY_MISMATCH",
    "CASE_MISMATCH",
    "PREFIX_MATCH",
    "SUBSTRING_MATCH",
}


def suggest_literal_binding_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    max_candidates: int = 2,
) -> list[dict[str, Any]]:
    patch_plan = structured_diagnosis.get("patch_plan") or {}
    if patch_plan.get("patch_goal") != "fix_literal_binding":
        return []

    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    failure_spec = structured_diagnosis.get("failure_spec") or {}
    query_spec = structured_diagnosis.get("query_spec") or {}
    root_cause = str(failure_spec.get("root_cause_type") or "UNKNOWN")
    if root_cause not in _LITERAL_ROOT_CAUSES:
        return []

    suggestions: list[dict[str, Any]] = []
    for fact in failure_spec.get("value_probe_facts") or []:
        replacement = _choose_safe_replacement(fact, root_cause=root_cause)
        if not replacement:
            continue
        expected_value = str(fact.get("expected_value") or "")
        target_columns = _resolve_target_columns(
            fact=fact,
            query_spec=query_spec,
            sql_text=sql_text,
        )
        if not target_columns:
            target_columns = [_normalize_column_text(str(fact.get("column") or ""))]
        if expected_value == replacement:
            continue
        for target_column in target_columns:
            if not target_column:
                continue
            repaired_sql = _replace_literal_for_column(
                sql_text=sql_text,
                target_column=target_column,
                expected_literal=expected_value,
                replacement_literal=replacement,
            )
            if not repaired_sql or repaired_sql == sql_text:
                continue
            suggestions.append(
                {
                    "sql": repaired_sql,
                    "operator": "literal_binding",
                    "target_column": target_column,
                    "expected_literal": expected_value,
                    "replacement_literal": replacement,
                    "root_cause": root_cause,
                    "repair_rationale": (
                        f"Replace literal {expected_value!r} on {target_column} with DB-backed value {replacement!r}."
                    ),
                }
            )
            if len(suggestions) >= max_candidates:
                break
        if len(suggestions) >= max_candidates:
            break

    deduped: list[dict[str, Any]] = []
    seen_sql: set[str] = set()
    for item in suggestions:
        sql = item["sql"]
        if sql in seen_sql:
            continue
        seen_sql.add(sql)
        deduped.append(item)
    return deduped


def _resolve_target_columns(
    *,
    fact: dict[str, Any],
    query_spec: dict[str, Any],
    sql_text: str,
) -> list[str]:
    expected = str(fact.get("expected_value") or "")
    probe_column = _normalize_column_text(str(fact.get("column") or ""))
    candidates = []
    for binding in query_spec.get("literal_bindings") or []:
        if str(binding.get("literal") or "") != expected:
            continue
        column = _normalize_column_text(str(binding.get("column") or ""))
        if not column:
            continue
        candidates.append(column)
    if candidates:
        return list(dict.fromkeys(candidates))
    return [probe_column] if probe_column else []


def _choose_safe_replacement(fact: dict[str, Any], *, root_cause: str) -> str | None:
    expected = str(fact.get("expected_value") or "").strip()
    closest = str(fact.get("closest_match") or "").strip()
    diagnosis = str(fact.get("diagnosis") or root_cause)
    actual_values = [str(v).strip() for v in fact.get("actual_distinct_values") or [] if str(v).strip()]
    if not expected:
        return None

    if diagnosis == "CASE_MISMATCH":
        for actual in actual_values:
            if actual.lower() == expected.lower() and actual != expected:
                return actual

    if closest and _is_safe_literal_replacement(expected, closest, diagnosis=diagnosis):
        return closest

    for actual in actual_values:
        if _is_safe_literal_replacement(expected, actual, diagnosis=diagnosis):
            return actual
    return None


def _is_safe_literal_replacement(expected: str, candidate: str, *, diagnosis: str) -> bool:
    expected = expected.strip()
    candidate = candidate.strip()
    if not expected or not candidate or expected == candidate:
        return False

    if diagnosis == "CASE_MISMATCH":
        return expected.lower() == candidate.lower()

    if _is_plain_integer(expected) and _is_plain_integer(candidate):
        return False

    if _looks_like_date(expected) or _looks_like_datetime(candidate):
        if candidate.startswith(expected) or expected.startswith(candidate):
            return True

    norm_expected = _normalize_literal(expected)
    norm_candidate = _normalize_literal(candidate)
    if not norm_expected or not norm_candidate:
        return False

    if norm_expected == norm_candidate:
        return True
    if diagnosis in {"PREFIX_MATCH", "SUBSTRING_MATCH"} and (
        norm_expected in norm_candidate or norm_candidate in norm_expected
    ):
        return True

    similarity = SequenceMatcher(None, norm_expected, norm_candidate).ratio()
    if similarity >= 0.95:
        return True

    return False


def _replace_literal_for_column(
    *,
    sql_text: str,
    target_column: str,
    expected_literal: str,
    replacement_literal: str,
) -> str:
    try:
        tree = parse_one(sql_text, read="sqlite")
    except Exception:
        return ""

    alias_to_table = _collect_alias_to_table(tree)
    replaced = {"count": 0}

    def _transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.ILike)):
            left = node.this
            right = node.expression
            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                if _column_matches(left, target_column, alias_to_table) and _literal_matches(
                    right, expected_literal
                ):
                    replaced["count"] += 1
                    node.set("expression", _build_literal_node(replacement_literal, right))
            elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                if _column_matches(right, target_column, alias_to_table) and _literal_matches(
                    left, expected_literal
                ):
                    replaced["count"] += 1
                    node.set("this", _build_literal_node(replacement_literal, left))
        elif isinstance(node, exp.In) and isinstance(node.this, exp.Column):
            if _column_matches(node.this, target_column, alias_to_table):
                new_values = []
                changed = False
                for item in node.expressions or []:
                    if isinstance(item, exp.Literal) and _literal_matches(item, expected_literal):
                        new_values.append(_build_literal_node(replacement_literal, item))
                        replaced["count"] += 1
                        changed = True
                    else:
                        new_values.append(item)
                if changed:
                    node.set("expressions", new_values)
        elif isinstance(node, exp.Between) and isinstance(node.this, exp.Column):
            if _column_matches(node.this, target_column, alias_to_table):
                low = node.args.get("low")
                high = node.args.get("high")
                if isinstance(low, exp.Literal) and _literal_matches(low, expected_literal):
                    node.set("low", _build_literal_node(replacement_literal, low))
                    replaced["count"] += 1
                if isinstance(high, exp.Literal) and _literal_matches(high, expected_literal):
                    node.set("high", _build_literal_node(replacement_literal, high))
                    replaced["count"] += 1
        return node

    updated = tree.transform(_transform)
    if replaced["count"] <= 0:
        return ""
    return updated.sql(dialect="sqlite")


def _collect_alias_to_table(tree: exp.Expression) -> dict[str, str]:
    alias_to_table: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        table_name = (table.name or "").lower()
        if not table_name:
            continue
        alias_to_table[table_name] = table_name
        alias = (table.alias or "").lower()
        if alias:
            alias_to_table[alias] = table_name
    return alias_to_table


def _column_matches(column: exp.Column, target_column: str, alias_to_table: dict[str, str]) -> bool:
    actual = _normalize_column(column, alias_to_table)
    return actual == _normalize_column_text(target_column)


def _normalize_column(column: exp.Column, alias_to_table: dict[str, str]) -> str:
    column_name = (column.name or "").lower()
    table_name = (column.table or "").lower()
    if table_name:
        table_name = alias_to_table.get(table_name, table_name)
        return f"{table_name}.{column_name}"
    return column_name


def _normalize_column_text(text: str) -> str:
    text = text.strip().strip("[]`\"").lower()
    if "." not in text:
        return text
    parts = [part.strip("[]`\"") for part in text.split(".")]
    return ".".join(part.lower() for part in parts if part)


def _literal_matches(node: exp.Literal, expected_literal: str) -> bool:
    value = node.this if node.is_string else str(node.this)
    return str(value) == str(expected_literal)


def _build_literal_node(replacement_literal: str, original: exp.Literal) -> exp.Literal:
    if original.is_string:
        return exp.Literal.string(replacement_literal)
    if _is_plain_integer(replacement_literal):
        return exp.Literal.number(replacement_literal)
    try:
        float(replacement_literal)
        return exp.Literal.number(replacement_literal)
    except Exception:
        return exp.Literal.string(replacement_literal)


def _normalize_literal(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _is_plain_integer(value: str) -> bool:
    return bool(re.fullmatch(r"\d+", value or ""))


def _looks_like_date(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""))


def _looks_like_datetime(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?", value or ""))
