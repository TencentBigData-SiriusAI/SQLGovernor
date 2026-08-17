from __future__ import annotations

import re
from typing import Any

from sqlglot import exp, parse_one


def suggest_date_anchor_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    max_candidates: int = 4,
) -> list[dict[str, Any]]:
    failure_spec = structured_diagnosis.get("failure_spec") or {}
    if not failure_spec.get("date_anchor_hints"):
        return []

    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    query_spec = structured_diagnosis.get("query_spec") or {}
    where_predicates = query_spec.get("where_predicates") or []
    subqueries = query_spec.get("subqueries") or []
    top_tables = {
        str(t).lower() for t in (query_spec.get("base_tables") or [])
    } | {
        str(j.get("table") or "").lower() for j in (query_spec.get("joins") or [])
    }

    alias_map = _collect_table_aliases(sql_text)
    suggestions: list[dict[str, Any]] = []

    for predicate in where_predicates:
        expr = str(predicate.get("expression") or "")
        parsed = _parse_simple_date_predicate(expr)
        if not parsed:
            continue
        lhs, op, literal = parsed
        current_column = _normalize_column_text(lhs)

        related_hints = [
            hint
            for hint in failure_spec.get("date_anchor_hints") or []
            if str(hint.get("literal") or "") == literal
        ]
        if not related_hints:
            continue

        for hint in related_hints:
            target_table = str(hint.get("table") or "").lower()
            target_column = str(hint.get("column") or "").lower()
            match_kind = str(hint.get("match_kind") or "")
            target_ref = _format_target_ref(target_table, target_column, alias_map)

            candidate_predicate = ""
            rule = ""

            if target_table == _split_column(current_column)[0] and target_column == _split_column(current_column)[1]:
                candidate_predicate, rule = _normalize_date_predicate(
                    lhs=target_ref,
                    op=op,
                    literal=literal,
                    match_kind=match_kind,
                )
            elif not subqueries and target_table in top_tables:
                candidate_predicate, rule = _reanchor_date_predicate(
                    target_ref=target_ref,
                    op=op,
                    literal=literal,
                    match_kind=match_kind,
                )

            if not candidate_predicate or candidate_predicate == expr:
                continue
            new_sql = _replace_predicate_via_ast(
                sql_text=sql_text,
                original_predicate=expr,
                replacement_predicate=candidate_predicate,
            )
            if new_sql == sql_text:
                continue
            suggestions.append(
                {
                    "sql": new_sql,
                    "operator": "date_anchor",
                    "rule": rule,
                    "original_predicate": expr,
                    "replacement_predicate": candidate_predicate,
                    "target_table": target_table,
                    "target_column": target_column,
                    "match_kind": match_kind,
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


def _parse_simple_date_predicate(expr: str) -> tuple[str, str, str] | None:
    match = re.match(
        r"^\s*(?P<lhs>.+?)\s*(?P<op>LIKE|=|>=|<=|>|<)\s*'(?P<literal>[^']+)'\s*$",
        expr,
        re.I,
    )
    if not match:
        return None
    return (
        match.group("lhs").strip(),
        match.group("op").upper(),
        match.group("literal").strip(),
    )


def _normalize_date_predicate(*, lhs: str, op: str, literal: str, match_kind: str) -> tuple[str, str]:
    month_key = _month_key(literal)
    exact_date = _normalize_exact_date(literal)

    if op in {">", ">=", "<", "<=", "="} and exact_date:
        return (
            f"CAST({lhs} AS TEXT) {op} '{exact_date}'",
            "date_comparison_normalization",
        )
    if op in {"LIKE", "="} and match_kind in {"month_key_exact", "month_key_from_date", "month_key_from_dashed_prefix"} and month_key:
        return (
            f"SUBSTR(REPLACE(CAST({lhs} AS TEXT), '-', ''), 1, 6) = '{month_key}'",
            "date_grain_normalization",
        )
    if op == "LIKE" and match_kind == "month_prefix_dashed" and literal.endswith("%"):
        bare = literal.rstrip("%")
        return (
            f"CAST({lhs} AS TEXT) LIKE '{bare}%'",
            "date_prefix_normalization",
        )
    if op == "LIKE" and match_kind == "date_prefix" and exact_date:
        return (
            f"CAST({lhs} AS TEXT) LIKE '{exact_date}%'",
            "exact_date_prefix_normalization",
        )
    return "", ""


def _reanchor_date_predicate(*, target_ref: str, op: str, literal: str, match_kind: str) -> tuple[str, str]:
    month_key = _month_key(literal)
    exact_date = _normalize_exact_date(literal)

    if op in {">", ">=", "<", "<=", "="} and exact_date:
        return (
            f"CAST({target_ref} AS TEXT) {op} '{exact_date}'",
            "date_column_reanchor",
        )
    if op in {"LIKE", "="} and match_kind in {"month_key_exact", "month_key_from_date", "month_key_from_dashed_prefix"} and month_key:
        return (
            f"SUBSTR(REPLACE(CAST({target_ref} AS TEXT), '-', ''), 1, 6) = '{month_key}'",
            "date_column_reanchor_with_month_key",
        )
    if op == "LIKE" and match_kind == "month_prefix_dashed" and literal.endswith("%"):
        bare = literal.rstrip("%")
        return (
            f"CAST({target_ref} AS TEXT) LIKE '{bare}%'",
            "date_column_reanchor_with_prefix",
        )
    if op == "LIKE" and match_kind == "date_prefix" and exact_date:
        return (
            f"CAST({target_ref} AS TEXT) LIKE '{exact_date}%'",
            "date_column_reanchor_with_prefix",
        )
    return "", ""


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


def _replace_predicate_via_ast(
    *,
    sql_text: str,
    original_predicate: str,
    replacement_predicate: str,
) -> str:
    try:
        tree = parse_one(sql_text, read="sqlite")
        wrapper = parse_one(
            f"SELECT * FROM __codex_tmp__ WHERE {replacement_predicate}",
            read="sqlite",
        )
    except Exception:
        return sql_text

    where_expr = wrapper.args.get("where")
    if where_expr is None or where_expr.this is None:
        return sql_text
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
        return sql_text
    return updated.sql(dialect="sqlite")


def _format_target_ref(table: str, column: str, alias_map: dict[str, str]) -> str:
    table_ref = alias_map.get(table.lower(), table)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column):
        return f"{table_ref}.{column}"
    return f'{table_ref}."{column}"'


def _normalize_exact_date(literal: str) -> str | None:
    text = literal.rstrip("%")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", text):
        parts = text.split("/")
        return f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    return None


def _month_key(literal: str) -> str | None:
    text = literal.rstrip("%")
    if re.fullmatch(r"\d{6}", text):
        return text
    if re.fullmatch(r"\d{4}-\d{2}", text):
        return text[:4] + text[5:7]
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text[:4] + text[5:7]
    return None


def _normalize_column_text(text: str) -> str:
    text = str(text or "").strip().strip("[]`\"").lower()
    if "." not in text:
        return text
    parts = [part.strip("[]`\"") for part in text.split(".")]
    return ".".join(part.lower() for part in parts if part)


def _split_column(column: str) -> tuple[str, str]:
    normalized = _normalize_column_text(column)
    if "." not in normalized:
        return "", normalized
    table, col = normalized.split(".", 1)
    return table, col


def _normalize_predicate_text(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())
