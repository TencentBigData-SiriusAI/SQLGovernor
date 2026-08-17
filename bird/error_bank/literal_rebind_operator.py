from __future__ import annotations

import sqlite3
from typing import Any

from sqlglot import exp, parse_one

from error_bank.value_search import search_literal_in_query_tables


_REBIND_ROOT_CAUSES = {
    "VALUE_NOT_EXISTS",
    "EXACT_MATCH_EXISTS",
    "FUZZY_MISMATCH",
    "CASE_MISMATCH",
    "PREFIX_MATCH",
    "SUBSTRING_MATCH",
}


def suggest_literal_rebind_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    db_path: str,
    max_candidates: int = 2,
    search_timeout_seconds: int = 5,
) -> list[dict[str, Any]]:
    patch_plan = structured_diagnosis.get("patch_plan") or {}
    if patch_plan.get("patch_goal") != "fix_literal_binding":
        return []

    failure_spec = structured_diagnosis.get("failure_spec") or {}
    root_cause = str(failure_spec.get("root_cause_type") or "UNKNOWN")
    if root_cause not in _REBIND_ROOT_CAUSES:
        return []

    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    query_spec = structured_diagnosis.get("query_spec") or {}
    task_spec = structured_diagnosis.get("task_spec") or {}
    failure_hints = failure_spec.get("in_query_value_hints") or []
    suggestions: list[dict[str, Any]] = []

    for binding in query_spec.get("literal_bindings") or []:
        current_column = str(binding.get("column") or "")
        expected_literal = str(binding.get("literal") or "")
        if not current_column or not expected_literal:
            continue

        target_locations = _collect_target_locations(
            failure_hints=failure_hints,
            db_path=db_path,
            query_spec=query_spec,
            task_spec=task_spec,
            current_column=current_column,
            expected_literal=expected_literal,
            search_timeout_seconds=search_timeout_seconds,
        )
        for target in target_locations[:max_candidates]:
            repaired_sql = _replace_bound_column(
                sql_text=sql_text,
                current_column=current_column,
                expected_literal=expected_literal,
                target_table=target["table"],
                target_column=target["column"],
            )
            if not repaired_sql or repaired_sql == sql_text:
                continue
            suggestions.append(
                {
                    "sql": repaired_sql,
                    "operator": "literal_rebind",
                    "root_cause": root_cause,
                    "current_column": current_column,
                    "target_column": f"{target['table']}.{target['column']}",
                    "expected_literal": expected_literal,
                    "repair_rationale": (
                        f"Rebind literal {expected_literal!r} from {current_column} "
                        f"to {target['table']}.{target['column']} based on in-query table search."
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
        if item["sql"] in seen_sql:
            continue
        seen_sql.add(item["sql"])
        deduped.append(item)
    return deduped


def _collect_target_locations(
    *,
    failure_hints: list[dict[str, Any]],
    db_path: str,
    query_spec: dict[str, Any],
    task_spec: dict[str, Any],
    current_column: str,
    expected_literal: str,
    search_timeout_seconds: int,
) -> list[dict[str, Any]]:
    hinted = []
    for item in failure_hints:
        if str(item.get("current_column") or "") != _normalize_column_text(current_column):
            continue
        if str(item.get("expected_literal") or "") != expected_literal:
            continue
        hinted.append(
            {
                "table": str(item.get("table") or "").lower(),
                "column": str(item.get("column") or "").lower(),
                "exact_matches": int(item.get("exact_matches") or 0),
                "score": int(item.get("score") or 0),
            }
        )
    candidates = hinted or _search_in_query_tables(
        db_path=db_path,
        query_spec=query_spec,
        task_spec=task_spec,
        current_column=current_column,
        expected_literal=expected_literal,
        search_timeout_seconds=search_timeout_seconds,
    )
    current_table, current_col_name = _split_column(current_column)
    filtered = []
    for item in candidates:
        target_table = str(item.get("table") or "").lower()
        target_col = str(item.get("column") or "").lower()
        if not target_table or not target_col:
            continue
        if target_table == current_table:
            if not _same_table_rebind_allowed(current_col_name, target_col):
                continue
            filtered.append(item)
            continue
        if target_col != current_col_name:
            continue
        if _tables_are_semantically_related(current_table, target_table):
            filtered.append(item)
    return filtered


def _search_in_query_tables(
    *,
    db_path: str,
    query_spec: dict[str, Any],
    task_spec: dict[str, Any],
    current_column: str,
    expected_literal: str,
    search_timeout_seconds: int,
) -> list[dict[str, Any]]:
    return search_literal_in_query_tables(
        db_path=db_path,
        query_spec=query_spec,
        task_spec=task_spec,
        current_column=current_column,
        expected_literal=expected_literal,
        timeout_seconds=search_timeout_seconds,
    )


def _replace_bound_column(
    *,
    sql_text: str,
    current_column: str,
    expected_literal: str,
    target_table: str,
    target_column: str,
) -> str:
    try:
        tree = parse_one(sql_text, read="sqlite")
    except Exception:
        return ""

    alias_to_table, table_to_alias = _collect_alias_maps(tree)
    replaced = {"count": 0}

    def _transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.ILike)):
            left = node.this
            right = node.expression
            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                if _column_matches(left, current_column, alias_to_table) and _literal_matches(right, expected_literal):
                    node.set("this", _build_target_column_node(target_table, target_column, table_to_alias))
                    replaced["count"] += 1
            elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                if _column_matches(right, current_column, alias_to_table) and _literal_matches(left, expected_literal):
                    node.set("expression", _build_target_column_node(target_table, target_column, table_to_alias))
                    replaced["count"] += 1
        elif isinstance(node, exp.In) and isinstance(node.this, exp.Column):
            if _column_matches(node.this, current_column, alias_to_table):
                literals = [item for item in node.expressions or [] if isinstance(item, exp.Literal)]
                if any(_literal_matches(item, expected_literal) for item in literals):
                    node.set("this", _build_target_column_node(target_table, target_column, table_to_alias))
                    replaced["count"] += 1
        elif isinstance(node, exp.Between) and isinstance(node.this, exp.Column):
            if _column_matches(node.this, current_column, alias_to_table):
                low = node.args.get("low")
                high = node.args.get("high")
                if (
                    isinstance(low, exp.Literal)
                    and _literal_matches(low, expected_literal)
                ) or (
                    isinstance(high, exp.Literal)
                    and _literal_matches(high, expected_literal)
                ):
                    node.set("this", _build_target_column_node(target_table, target_column, table_to_alias))
                    replaced["count"] += 1
        return node

    updated = tree.transform(_transform)
    if replaced["count"] <= 0:
        return ""
    return updated.sql(dialect="sqlite")


def _collect_alias_maps(tree: exp.Expression) -> tuple[dict[str, str], dict[str, str]]:
    alias_to_table: dict[str, str] = {}
    table_to_alias: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        table_name = (table.name or "").lower()
        if not table_name:
            continue
        alias_to_table[table_name] = table_name
        alias = (table.alias or "").lower()
        if alias:
            alias_to_table[alias] = table_name
            table_to_alias.setdefault(table_name, alias)
        else:
            table_to_alias.setdefault(table_name, table_name)
    return alias_to_table, table_to_alias


def _column_matches(column: exp.Column, current_column: str, alias_to_table: dict[str, str]) -> bool:
    actual = _normalize_column(column, alias_to_table)
    return actual == _normalize_column_text(current_column)


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


def _build_target_column_node(target_table: str, target_column: str, table_to_alias: dict[str, str]) -> exp.Column:
    table_ref = table_to_alias.get(target_table.lower(), target_table)
    return exp.column(target_column, table=table_ref)


def _split_column(column: str) -> tuple[str, str]:
    normalized = _normalize_column_text(column)
    if "." not in normalized:
        return "", normalized
    table, col = normalized.split(".", 1)
    return table, col


def _tables_are_semantically_related(left: str, right: str) -> bool:
    left_tokens = _table_tokens(left)
    right_tokens = _table_tokens(right)
    return bool(left_tokens & right_tokens)


def _table_tokens(table: str) -> set[str]:
    tokens = set()
    for part in str(table or "").lower().split("_"):
        if not part:
            continue
        tokens.add(part)
        if part.endswith("s") and len(part) > 3:
            tokens.add(part[:-1])
    return tokens


def _same_table_rebind_allowed(current_col: str, target_col: str) -> bool:
    current_col = str(current_col or "").lower()
    target_col = str(target_col or "").lower()
    if not current_col or not target_col or current_col == target_col:
        return False
    if len(current_col) >= 4 and current_col in target_col:
        return True
    if len(target_col) >= 4 and target_col in current_col:
        return True
    return False
