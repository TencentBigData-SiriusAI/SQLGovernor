from __future__ import annotations

from typing import Any

from sqlglot import exp, parse_one

from error_bank.date_anchor_operator import (
    _normalize_date_predicate,
    _parse_simple_date_predicate,
)


def suggest_entity_anchor_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    max_candidates: int = 2,
) -> list[dict[str, Any]]:
    alignment_spec = structured_diagnosis.get("alignment_spec") or {}
    errors = alignment_spec.get("alignment_errors") or []
    if not any(err.get("misalignment_type") == "entity_anchor_slot_misaligned" for err in errors):
        return []

    task_spec = structured_diagnosis.get("task_spec") or {}
    query_spec = structured_diagnosis.get("query_spec") or {}
    failure_spec = structured_diagnosis.get("failure_spec") or {}
    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    anchor_hints = failure_spec.get("anchor_swap_hints") or []
    if not anchor_hints:
        return []

    projection_candidates = query_spec.get("projections") or []
    if len(projection_candidates) != 1:
        return []
    projection_sources = projection_candidates[0].get("source_columns") or []
    if len(projection_sources) != 1 or not projection_sources[0].lower().endswith(".id"):
        return []

    date_predicates = [
        item for item in (query_spec.get("where_predicates") or [])
        if any("date" in col.lower() or "time" in col.lower() for col in item.get("columns") or [])
    ]
    best_date_hint = _choose_best_date_hint(
        date_hints=failure_spec.get("date_anchor_hints") or [],
        value_predicates=query_spec.get("where_predicates") or [],
    )

    suggestions: list[dict[str, Any]] = []
    for hint in anchor_hints[:max_candidates]:
        target_table = str(hint.get("target_table") or "").lower()
        path = hint.get("path") or []
        if not target_table or not path:
            continue
        new_sql = _apply_entity_anchor_patch(
            sql_text=sql_text,
            target_table=target_table,
            path=path,
            date_predicates=date_predicates,
            best_date_hint=best_date_hint,
        )
        if not new_sql or new_sql == sql_text:
            continue
        suggestions.append(
            {
                "sql": new_sql,
                "operator": "entity_anchor",
                "rule": "insert_target_entity_join_and_swap_id_projection",
                "target_table": target_table,
                "path": path,
            }
        )
        swapped_sql = _apply_entity_base_swap_patch(
            sql_text=sql_text,
            target_table=target_table,
            path=path,
            date_predicates=date_predicates,
            best_date_hint=best_date_hint,
        )
        if swapped_sql and swapped_sql != sql_text and swapped_sql != new_sql:
            suggestions.append(
                {
                    "sql": swapped_sql,
                    "operator": "entity_anchor",
                    "rule": "swap_base_table_to_target_entity_and_reanchor_date",
                    "target_table": target_table,
                    "path": path,
                }
            )
    return suggestions


def _apply_entity_anchor_patch(
    *,
    sql_text: str,
    target_table: str,
    path: list[dict[str, Any]],
    date_predicates: list[dict[str, Any]],
    best_date_hint: dict[str, Any] | None,
) -> str:
    try:
        tree = parse_one(sql_text, read="sqlite")
    except Exception:
        return ""
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return ""

    alias_map = _collect_aliases(tree)
    target_alias = alias_map.get(target_table, target_table[:1] or "t")
    if not any(edge.get("to_table") == target_table for edge in path):
        return ""

    first_edge = path[0]
    source_table = str(first_edge.get("from_table") or "").lower()
    source_col = str(first_edge.get("from_column") or "")
    target_col = str(first_edge.get("to_column") or "")
    source_alias = alias_map.get(source_table, source_table[:1] or "t")

    join_sql = (
        f"SELECT * FROM __tmp__ JOIN {target_table} AS {target_alias} "
        f"ON {source_alias}.{source_col} = {target_alias}.{target_col}"
    )
    join_expr = parse_one(join_sql, read="sqlite").find(exp.Join)
    if join_expr is None:
        return ""
    existing_joins = list(select.args.get("joins") or [])
    if not any(_same_join_target(item, target_table) for item in existing_joins):
        existing_joins.append(join_expr)
        select.set("joins", existing_joins)

    # Swap the single ID projection to the target entity table.
    select.set("expressions", [exp.column("ID", table=target_alias)])

    if date_predicates and best_date_hint:
        original_expr = str(date_predicates[0].get("expression") or "")
        parsed = _parse_simple_date_predicate(original_expr)
        if parsed:
            _, op, literal = parsed
            lhs = f'{alias_map.get(str(best_date_hint.get("table") or "").lower(), str(best_date_hint.get("table") or "").lower())}.{best_date_hint.get("column")}'
            replacement_predicate, _ = _normalize_date_predicate(
                lhs=lhs,
                op=op,
                literal=literal,
                match_kind=str(best_date_hint.get("match_kind") or ""),
            )
            if replacement_predicate:
                updated = _replace_predicate(tree, original_expr, replacement_predicate)
                if updated:
                    return updated
    return tree.sql(dialect="sqlite")


def _apply_entity_base_swap_patch(
    *,
    sql_text: str,
    target_table: str,
    path: list[dict[str, Any]],
    date_predicates: list[dict[str, Any]],
    best_date_hint: dict[str, Any] | None,
) -> str:
    try:
        tree = parse_one(sql_text, read="sqlite")
    except Exception:
        return ""
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return ""

    from_expr = select.args.get("from")
    if from_expr is None or from_expr.this is None or not isinstance(from_expr.this, exp.Table):
        return ""

    alias_map = _collect_aliases(tree)
    target_alias = target_table[:1] or "t"
    first_edge = path[0]
    current_table = str(first_edge.get("from_table") or "").lower()
    current_alias = alias_map.get(current_table, current_table[:1] or "t")
    current_col = str(first_edge.get("from_column") or "ID")
    target_col = str(first_edge.get("to_column") or "ID")

    # Swap the base table itself.
    from_expr.set(
        "this",
        exp.Table(this=exp.to_identifier(target_table), alias=exp.TableAlias(this=exp.to_identifier(target_alias))),
    )

    # Redirect the single ID projection to the target table.
    select.set("expressions", [exp.column(target_col, table=target_alias)])

    # Rewrite join predicates that still refer to the old base alias key.
    replaced_join = {"count": 0}

    def _transform_join_refs(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column):
            table_name = (node.table or "").lower()
            col_name = (node.name or "").lower()
            if table_name == current_alias.lower() and col_name == current_col.lower():
                replaced_join["count"] += 1
                return exp.column(target_col, table=target_alias)
        return node

    tree = tree.transform(_transform_join_refs)

    if date_predicates and best_date_hint:
        original_expr = str(date_predicates[0].get("expression") or "")
        parsed = _parse_simple_date_predicate(original_expr)
        if parsed:
            _, op, literal = parsed
            date_table = str(best_date_hint.get("table") or "").lower()
            date_column = str(best_date_hint.get("column") or "")
            date_alias = alias_map.get(date_table, date_table[:1] or "t")
            if date_table == target_table:
                date_alias = target_alias
            lhs = f"{date_alias}.{date_column}"
            replacement_predicate, _ = _normalize_date_predicate(
                lhs=lhs,
                op=op,
                literal=literal,
                match_kind=str(best_date_hint.get("match_kind") or ""),
            )
            if replacement_predicate:
                updated = _replace_predicate(tree, original_expr, replacement_predicate)
                if updated:
                    return updated
    return tree.sql(dialect="sqlite")


def _choose_best_date_hint(*, date_hints: list[dict[str, Any]], value_predicates: list[dict[str, Any]]) -> dict[str, Any] | None:
    value_tables = set()
    for predicate in value_predicates:
        for column in predicate.get("columns") or []:
            if "." in str(column):
                value_tables.add(str(column).split(".", 1)[0].lower())
    ranked = sorted(
        date_hints,
        key=lambda item: (
            1 if str(item.get("table") or "").lower() in value_tables else 0,
            int(item.get("score") or 0),
            int(item.get("match_count") or 0),
        ),
        reverse=True,
    )
    return ranked[0] if ranked else None


def _replace_predicate(tree: exp.Expression, original_predicate: str, replacement_predicate: str) -> str:
    wrapper = parse_one(f"SELECT * FROM __tmp__ WHERE {replacement_predicate}", read="sqlite")
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


def _collect_aliases(tree: exp.Expression) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        table_name = (table.name or "").lower()
        if not table_name:
            continue
        alias = (table.alias or "").lower()
        mapping[table_name] = alias or table_name
    return mapping


def _same_join_target(join_expr: exp.Join, target_table: str) -> bool:
    table_expr = join_expr.this
    if isinstance(table_expr, exp.Table):
        return str(table_expr.name or "").lower() == target_table.lower()
    return False
