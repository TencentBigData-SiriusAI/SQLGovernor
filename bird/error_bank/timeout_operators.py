from __future__ import annotations

import re
from typing import Any


def suggest_timeout_repairs(sql_text: str, *, max_candidates: int = 3) -> list[dict[str, Any]]:
    sql = (sql_text or "").strip().rstrip(";")
    if not sql:
        return []

    suggestions: list[dict[str, Any]] = []
    for fn in (
        _suggest_preaggregate_having_count,
        _suggest_limit_pushdown_correlated_subqueries,
        _suggest_or_join_unpivot,
    ):
        try:
            candidate = fn(sql)
        except Exception:
            candidate = None
        if not candidate:
            continue
        suggestions.append(candidate)
        if len(suggestions) >= max_candidates:
            break
    return suggestions


def _suggest_preaggregate_having_count(sql: str) -> dict[str, Any] | None:
    pattern = re.compile(
        r"""^SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<ltable>`?\w+`?)\s+(?P<lalias>\w+)\s+JOIN\s+(?P<rtable>`?\w+`?)\s+(?P<ralias>\w+)\s+ON\s+(?P<ljoin>\w+\.\w+)\s*=\s*(?P<rjoin>\w+\.\w+)\s+WHERE\s+(?P<where>.+?)\s+GROUP\s+BY\s+(?P<group>.+?)\s+HAVING\s+COUNT\((?P<count_expr>\w+\.\w+)\)\s*=\s*1$""",
        re.I | re.S,
    )
    m = pattern.match(_squash(sql))
    if not m:
        return None
    select = m.group("select")
    ltable = m.group("ltable")
    lalias = m.group("lalias")
    rtable = m.group("rtable")
    ralias = m.group("ralias")
    ljoin = m.group("ljoin")
    rjoin = m.group("rjoin")
    where = m.group("where")
    count_expr = m.group("count_expr")
    right_join_col = rjoin.split(".", 1)[1]
    preagg_alias = "agg"
    new_sql = (
        f"SELECT {select} "
        f"FROM {ltable} {lalias} "
        f"JOIN (SELECT {right_join_col} FROM {rtable} {ralias} "
        f"GROUP BY {right_join_col} HAVING COUNT({count_expr}) = 1) AS {preagg_alias} "
        f"ON {ljoin} = {preagg_alias}.{right_join_col} "
        f"WHERE {where}"
    )
    return {
        "sql": new_sql,
        "operator": "timeout_preaggregate",
        "rule": "preaggregate_join_side_before_groupby",
    }


def _suggest_limit_pushdown_correlated_subqueries(sql: str) -> dict[str, Any] | None:
    pattern = re.compile(
        r"""^SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<table>`?\w+`?)\s+AS\s+(?P<alias>\w+)\s+ORDER\s+BY\s+(?P=alias)\.(?P<order_col>\w+)\s+DESC\s+LIMIT\s+1$""",
        re.I | re.S,
    )
    m = pattern.match(_squash(sql))
    if not m:
        return None
    select_expr = m.group("select")
    table = m.group("table")
    alias = m.group("alias")
    order_col = m.group("order_col")
    if "SELECT" not in select_expr.upper():
        return None
    rewritten_select = select_expr.replace(f"{alias}.", "target_row.")
    new_sql = (
        f"WITH target_row AS (SELECT * FROM {table} AS {alias} ORDER BY {alias}.{order_col} DESC LIMIT 1) "
        f"SELECT {rewritten_select} FROM target_row"
    )
    return {
        "sql": new_sql,
        "operator": "timeout_limit_pushdown",
        "rule": "push_limit_before_correlated_scalar_subqueries",
    }


def _suggest_or_join_unpivot(sql: str) -> dict[str, Any] | None:
    pattern = re.compile(
        r"""^SELECT\s+DISTINCT\s+(?P<proj>\w+\.\w+)\s+FROM\s+(?P<ltable>`?\w+`?)\s+AS\s+(?P<lalias>\w+)\s+JOIN\s+(?P<rtable>`?\w+`?)\s+AS\s+(?P<ralias>\w+)\s+ON\s+(?P<or_join>.+?)\s+JOIN\s+(?P<ftable>`?\w+`?)\s+AS\s+(?P<falias>\w+)\s+ON\s+(?P<join2>.+?)\s+WHERE\s+(?P<filter>.+)$""",
        re.I | re.S,
    )
    m = pattern.match(_squash(sql))
    if not m:
        return None

    proj = m.group("proj")
    ltable = m.group("ltable")
    lalias = m.group("lalias")
    rtable = m.group("rtable")
    ralias = m.group("ralias")
    or_join = m.group("or_join")
    ftable = m.group("ftable")
    falias = m.group("falias")
    join2 = m.group("join2")
    filter_expr = m.group("filter")

    clauses = [part.strip() for part in re.split(r"\s+OR\s+", or_join, flags=re.I) if part.strip()]
    left_col = None
    right_cols: list[str] = []
    for clause in clauses:
        mm = re.match(rf"(?P<left>{lalias}\.\w+)\s*=\s*(?P<right>{ralias}\.\w+)", clause)
        if not mm:
            mm = re.match(rf"(?P<right>{ralias}\.\w+)\s*=\s*(?P<left>{lalias}\.\w+)", clause)
        if not mm:
            return None
        left = mm.group("left")
        right = mm.group("right")
        if left_col is None:
            left_col = left
        elif left_col != left:
            return None
        right_cols.append(right.split(".", 1)[1])
    if not left_col or len(right_cols) < 2:
        return None

    union_parts = [
        f"SELECT {ralias}.{col} AS join_key FROM {rtable} AS {ralias} JOIN {ftable} AS {falias} ON {join2} WHERE {filter_expr}"
        for col in right_cols
    ]
    new_sql = (
        "WITH exploded_keys AS ("
        + " UNION ".join(union_parts)
        + f") SELECT DISTINCT {proj} FROM {ltable} AS {lalias} "
        + f"JOIN exploded_keys ek ON {left_col} = ek.join_key"
    )
    return {
        "sql": new_sql,
        "operator": "timeout_or_join_unpivot",
        "rule": "rewrite_or_join_as_union_keyset",
    }


def _squash(sql: str) -> str:
    return " ".join((sql or "").strip().rstrip(";").split())
