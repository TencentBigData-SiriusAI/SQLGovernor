from __future__ import annotations

from typing import Any

from error_bank.aggregate_scope_operator import suggest_aggregate_scope_repairs
from error_bank.date_anchor_join_operator import suggest_date_anchor_join_repairs
from error_bank.literal_binding_operator import suggest_literal_binding_repairs
from error_bank.literal_rebind_operator import suggest_literal_rebind_repairs
from error_bank.translation_semantics_operator import suggest_translation_semantics_repairs
from error_bank.where_predicate_operator import suggest_where_predicate_repairs


def suggest_high_precision_empty_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    db_path: str,
    search_timeout_seconds: int = 5,
) -> list[dict[str, Any]]:
    suggestions = []
    for fn in (
        suggest_literal_binding_repairs,
        lambda sd: suggest_literal_rebind_repairs(
            sd,
            db_path=db_path,
            search_timeout_seconds=search_timeout_seconds,
        ),
        suggest_where_predicate_repairs,
        lambda sd: suggest_date_anchor_join_repairs(sd, db_path=db_path),
        suggest_translation_semantics_repairs,
        suggest_aggregate_scope_repairs,
    ):
        try:
            out = fn(structured_diagnosis)
        except Exception:
            out = []
        suggestions.extend(out or [])

    deduped = []
    seen = set()
    for item in suggestions:
        sql = (item.get("sql") or "").strip()
        if not sql:
            continue
        norm = " ".join(sql.split()).strip().lower()
        if norm in seen:
            continue
        seen.add(norm)
        deduped.append(item)
    return deduped
