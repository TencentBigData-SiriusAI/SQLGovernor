from __future__ import annotations

import re
import sqlite3
from typing import Any

from sqlglot import exp, parse_one

from error_bank.date_anchor_operator import (
    _normalize_date_predicate,
    _parse_simple_date_predicate,
    _normalize_column_text,
)


_KEYISH_NAMES = {
    "customerid",
    "gasstationid",
    "productid",
    "userid",
    "postid",
    "teamid",
    "driverid",
    "setcode",
    "code",
    "uuid",
}


def suggest_date_anchor_join_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    db_path: str,
    max_candidates: int = 4,
) -> list[dict[str, Any]]:
    failure_spec = structured_diagnosis.get("failure_spec") or {}
    date_hints = failure_spec.get("date_anchor_hints") or []
    if not date_hints:
        return []

    query_spec = structured_diagnosis.get("query_spec") or {}
    where_predicates = query_spec.get("where_predicates") or []
    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    current_tables = [
        str(table).lower()
        for table in (query_spec.get("base_tables") or [])
        if str(table).strip()
    ]
    current_tables += [
        str(join.get("table") or "").lower()
        for join in (query_spec.get("joins") or [])
        if str(join.get("table") or "").strip()
    ]
    current_tables = list(dict.fromkeys(current_tables))

    target_hints = []
    for hint in date_hints:
        target_table = str(hint.get("table") or "").lower()
        if not target_table or target_table in current_tables:
            continue
        target_hints.append(hint)
    if not target_hints:
        return []

    alias_map = _collect_table_aliases(sql_text)
    suggestions: list[dict[str, Any]] = []

    for predicate in where_predicates:
        expr = str(predicate.get("expression") or "")
        parsed = _parse_simple_date_predicate(expr)
        if not parsed:
            continue
        _, _, literal = parsed

        for hint in target_hints:
            if str(hint.get("literal") or "").rstrip("%") != literal.rstrip("%"):
                continue
            target_table = str(hint.get("table") or "").lower()
            target_column = str(hint.get("column") or "").lower()
            join_plan = _find_join_plan(
                db_path=db_path,
                current_tables=current_tables,
                target_table=target_table,
                alias_map=alias_map,
            )
            if not join_plan:
                continue
            new_sql = _apply_join_and_reanchor(
                sql_text=sql_text,
                original_predicate=expr,
                replacement_table=target_table,
                replacement_column=target_column,
                match_kind=str(hint.get("match_kind") or ""),
                alias_map=alias_map,
                join_plan=join_plan,
            )
            if not new_sql or new_sql == sql_text:
                continue
            suggestions.append(
                {
                    "sql": new_sql,
                    "operator": "date_anchor_join",
                    "rule": "insert_missing_date_anchor_table_and_rebind_date_predicate",
                    "original_predicate": expr,
                    "target_table": target_table,
                    "target_column": target_column,
                    "match_kind": str(hint.get("match_kind") or ""),
                    "join_plan": join_plan,
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
        if len(deduped) >= max_candidates:
            break
    return deduped


def _apply_join_and_reanchor(
    *,
    sql_text: str,
    original_predicate: str,
    replacement_table: str,
    replacement_column: str,
    match_kind: str,
    alias_map: dict[str, str],
    join_plan: dict[str, str],
) -> str:
    try:
        tree = parse_one(sql_text, read="sqlite")
    except Exception:
        return ""

    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return ""

    target_alias = _choose_alias(replacement_table, alias_map)
    join_sql = (
        f'SELECT * FROM __tmp__ JOIN {replacement_table} AS {target_alias} '
        f'ON {join_plan["left_ref"]} = {target_alias}.{join_plan["right_column"]}'
    )
    try:
        join_expr = parse_one(join_sql, read="sqlite").find(exp.Join)
    except Exception:
        return ""
    if join_expr is None:
        return ""

    existing_joins = list(select.args.get("joins") or [])
    if not any(_same_join_target(item, replacement_table) for item in existing_joins):
        existing_joins.append(join_expr)
        select.set("joins", existing_joins)

    parsed = _parse_simple_date_predicate(original_predicate)
    if not parsed:
        return ""
    _, op, literal = parsed
    replacement_ref = f"{target_alias}.{replacement_column}"
    replacement_predicate, _ = _normalize_date_predicate(
        lhs=replacement_ref,
        op=op,
        literal=literal,
        match_kind=match_kind,
    )
    if not replacement_predicate:
        return ""

    wrapper = parse_one(
        f"SELECT * FROM __tmp__ WHERE {replacement_predicate}",
        read="sqlite",
    )
    where_expr = wrapper.args.get("where")
    if where_expr is None or where_expr.this is None:
        return ""
    replacement_node = where_expr.this
    target_norm = " ".join(original_predicate.lower().split())
    replaced = {"count": 0}

    def _transform(node: exp.Expression) -> exp.Expression:
        if " ".join(node.sql(dialect="sqlite").lower().split()) == target_norm:
            replaced["count"] += 1
            return replacement_node.copy()
        return node

    updated = tree.transform(_transform)
    if replaced["count"] <= 0:
        return ""
    return updated.sql(dialect="sqlite")


def _find_join_plan(
    *,
    db_path: str,
    current_tables: list[str],
    target_table: str,
    alias_map: dict[str, str],
) -> dict[str, str] | None:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    try:
        target_cols = _get_table_columns(conn, target_table)
        for current_table in current_tables:
            current_cols = _get_table_columns(conn, current_table)
            shared = _shared_join_columns(current_cols, target_cols)
            if not shared:
                continue
            join_column = shared[0]
            return {
                "left_table": current_table,
                "right_table": target_table,
                "left_column": join_column,
                "right_column": join_column,
                "left_ref": f'{alias_map.get(current_table, current_table)}.{join_column}',
            }
    finally:
        conn.close()
    return None


def _shared_join_columns(left: list[str], right: list[str]) -> list[str]:
    left_set = {str(x).lower() for x in left}
    right_set = {str(x).lower() for x in right}
    shared = [col for col in left_set & right_set if _is_join_key(col)]
    return sorted(shared, key=_join_key_score, reverse=True)


def _is_join_key(column: str) -> bool:
    col = str(column or "").lower()
    if col in _KEYISH_NAMES:
        return True
    return col.endswith("id") or col.endswith("_id") or col.endswith("code") or col.endswith("_code")


def _join_key_score(column: str) -> int:
    if column in {"customerid", "userid", "patientid", "teamid", "driverid"}:
        return 100
    if column in {"gasstationid", "productid", "postid"}:
        return 90
    if column in {"code", "setcode", "uuid"}:
        return 80
    return 50


def _get_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    try:
        rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    except Exception:
        return []
    cols = []
    for row in rows:
        if len(row) > 1 and row[1]:
            cols.append(str(row[1]).lower())
    return cols


def _collect_table_aliases(sql_text: str) -> dict[str, str]:
    try:
        tree = parse_one(sql_text, read="sqlite")
    except Exception:
        return {}
    mapping: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        table_name = (table.name or "").lower()
        if not table_name:
            continue
        alias = (table.alias or "").lower()
        mapping[table_name] = alias or table_name
    return mapping


def _choose_alias(table: str, alias_map: dict[str, str]) -> str:
    base = alias_map.get(table.lower(), table.lower())
    if re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", base):
        return base
    return table.lower()


def _same_join_target(join_expr: exp.Join, target_table: str) -> bool:
    table_expr = join_expr.this
    if isinstance(table_expr, exp.Table):
        return str(table_expr.name or "").lower() == target_table.lower()
    return False
