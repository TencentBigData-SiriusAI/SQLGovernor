"""Generic SQL-vs-spec consistency checks."""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp

from .types import PathPlan, StructuredPlan


def evaluate_sql_against_plan(
    sql_text: str,
    *,
    plan: StructuredPlan,
    path: PathPlan,
) -> list[str]:
    if not sql_text.strip():
        return ["EMPTY_SQL"]

    try:
        tree = sqlglot.parse_one(sql_text, read="sqlite")
    except Exception as exc:
        return [f"parse_error:{exc}"]

    issues: list[str] = []
    tables = {table.name for table in tree.find_all(exp.Table) if table.name}
    columns = {column.name for column in tree.find_all(exp.Column) if column.name}

    if path.path_kind != "freeform":
        extra_tables = sorted(tables - set(path.tables))
        if extra_tables:
            issues.append("extra_tables:" + ",".join(extra_tables))

    select_exprs = list(tree.selects)
    intent = plan.answer_intent
    if intent and intent.return_slot_count is not None and len(select_exprs) != intent.return_slot_count:
        issues.append(f"answer_shape_slot_count_mismatch:{len(select_exprs)}!={intent.return_slot_count}")

    output_columns = [item for item in plan.output_spec.columns if item]
    if output_columns:
        output_names = {item.split(".")[-1].strip("`\"") for item in output_columns}
        select_tokens = {
            _select_token(expr)
            for expr in select_exprs
            if _select_token(expr)
        }
        missing = sorted(name for name in output_names if name not in select_tokens)
        if missing:
            issues.append("missing_output:" + ",".join(missing))
        if len(select_exprs) > len(output_columns):
            issues.append(f"projection_extra:{len(select_exprs)}>{len(output_columns)}")

    if intent and intent.return_slots:
        select_sqls = [expr.sql(dialect="sqlite").lower() for expr in select_exprs]
        for slot in intent.return_slots:
            slot_type = (slot.value_type or "").lower()
            if slot_type == "code" and any("type" in sql or "name" in sql or "description" in sql for sql in select_sqls):
                issues.append("code_slot_rendered_as_description")
                break
            if slot_type == "id" and any("name" in sql or "description" in sql for sql in select_sqls):
                issues.append("id_slot_rendered_as_description")
                break
            if slot_type == "percentage" and not _uses_float_division(tree):
                issues.append("percentage_slot_without_float_guard")
                break

    aggregate = plan.aggregate_spec
    if aggregate and aggregate.function:
        if not _has_aggregate(tree, aggregate.function):
            issues.append(f"missing_aggregate:{aggregate.function}")
        agg_column = aggregate.column.split(".")[-1].strip("`\"")
        if agg_column and agg_column not in columns:
            issues.append(f"aggregate_column_missing:{agg_column}")

    if plan.filter_spec:
        where_text = _clause_text(tree.args.get("where"))
        having_text = _clause_text(tree.args.get("having"))
        predicate_text = f"{where_text}\n{having_text}".lower()
        for filter_spec in plan.filter_spec:
            col_name = filter_spec.column.split(".")[-1].strip("`\"").lower()
            if col_name and col_name not in predicate_text:
                issues.append(f"missing_filter:{col_name}")

    ordering = plan.ordering_spec
    if ordering:
        order_clause = tree.args.get("order")
        if order_clause is None:
            issues.append("missing_order_by")
        else:
            order_text = order_clause.sql().lower()
            order_col = ordering.column.split(".")[-1].strip("`\"").lower()
            if order_col and order_col not in order_text:
                issues.append(f"order_column_mismatch:{order_col}")
        if ordering.limit is not None:
            limit_clause = tree.args.get("limit")
            if limit_clause is None:
                issues.append("missing_limit")

    if _question_requires_percentage(plan) and not _uses_float_division(tree):
        issues.append("percentage_without_float_guard")

    return issues


def _select_token(expr: exp.Expression) -> str | None:
    alias = expr.alias_or_name
    if alias:
        return alias.strip("`\"")
    if isinstance(expr, exp.Column):
        return expr.name.strip("`\"")
    sql_text = expr.sql()
    return sql_text.split(".")[-1].strip("`\"") if sql_text else None


def _has_aggregate(tree: exp.Expression, function_name: str) -> bool:
    target = function_name.upper()
    aggregate_map = {
        "AVG": exp.Avg,
        "COUNT": exp.Count,
        "SUM": exp.Sum,
        "MAX": exp.Max,
        "MIN": exp.Min,
    }
    if target in aggregate_map:
        return any(isinstance(node, aggregate_map[target]) for node in tree.walk())
    return target.lower() in tree.sql(dialect="sqlite").lower()


def _clause_text(clause: Any) -> str:
    if clause is None:
        return ""
    try:
        return clause.sql()
    except Exception:
        return str(clause)


def _question_requires_percentage(plan: StructuredPlan) -> bool:
    text = f"{plan.question} {plan.evidence}".lower()
    return any(token in text for token in ["percent", "percentage", "%", "ratio", "proportion"])


def _uses_float_division(tree: exp.Expression) -> bool:
    sql_text = tree.sql(dialect="sqlite").lower()
    return "cast(" in sql_text or "1.0" in sql_text or "round(" in sql_text
