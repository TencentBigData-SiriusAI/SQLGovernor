from __future__ import annotations

import re
from typing import Any


def suggest_translation_semantics_repairs(
    structured_diagnosis: dict[str, Any],
    *,
    max_candidates: int = 2,
) -> list[dict[str, Any]]:
    task_spec = structured_diagnosis.get("task_spec") or {}
    question = str(task_spec.get("question") or structured_diagnosis.get("question") or "")
    evidence = str(task_spec.get("evidence") or structured_diagnosis.get("evidence") or "")
    text = f"{question} {evidence}".lower()
    sql_text = (structured_diagnosis.get("original_sql") or "").strip()
    if not sql_text:
        return []

    query_spec = structured_diagnosis.get("query_spec") or {}
    tables = {str(t).lower() for t in (query_spec.get("base_tables") or [])}
    tables |= {str(j.get("table") or "").lower() for j in (query_spec.get("joins") or [])}
    # We only attempt this family when translation-like tables are already in play or clearly available.
    if not any(token in text for token in ["language", "translation", "japanese", "korean", "version"]):
        return []

    literals = _extract_quoted_literals(question + " " + evidence)
    pattern = _detect_pattern(text)
    if pattern is None:
        return []

    candidates: list[dict[str, Any]] = []
    if pattern == "localized_existence":
        card_name = next((literal for literal in literals if literal.lower() not in {"korean", "japanese"}), "")
        lang = next((literal for literal in literals if literal.lower() in {"korean", "japanese"}), "Korean")
        if card_name:
            candidates.append(
                {
                    "sql": (
                        "SELECT IIF(SUM(CASE WHEN st.language = '{lang}' AND st.translation IS NOT NULL THEN 1 ELSE 0 END) > 0, 'YES', 'NO') "
                        "FROM cards AS c INNER JOIN set_translations AS st ON st.setCode = c.setCode "
                        "WHERE c.name = '{card}'"
                    ).format(lang=lang, card=card_name.replace("'", "''")),
                    "operator": "translation_semantics",
                    "rule": "localized_existence_via_set_translations",
                }
            )
    elif pattern == "localized_translation_by_card":
        card_name = next((literal for literal in literals if literal.lower() not in {"korean", "japanese"}), "")
        lang = next((literal for literal in literals if literal.lower() in {"korean", "japanese"}), "Japanese")
        if card_name:
            candidates.append(
                {
                    "sql": (
                        "SELECT st.translation FROM cards AS c INNER JOIN set_translations AS st ON st.setCode = c.setCode "
                        "WHERE c.name = '{card}' AND st.language = '{lang}' AND st.translation IS NOT NULL"
                    ).format(card=card_name.replace("'", "''"), lang=lang),
                    "operator": "translation_semantics",
                    "rule": "localized_translation_via_set_translations_not_null",
                }
            )
    elif pattern == "language_by_set_name":
        set_name = next((literal for literal in literals if literal.lower() not in {"language", "set"}), "")
        if set_name:
            candidates.append(
                {
                    "sql": (
                        "SELECT language FROM set_translations WHERE id IN "
                        "(SELECT id FROM sets WHERE name = '{name}')"
                    ).format(name=set_name.replace("'", "''")),
                    "operator": "translation_semantics",
                    "rule": "language_lookup_via_set_id_subquery",
                }
            )

    deduped = []
    seen = set()
    for item in candidates:
        sql = " ".join((item.get("sql") or "").split()).strip()
        if not sql or sql.lower() in seen:
            continue
        seen.add(sql.lower())
        deduped.append(item)
        if len(deduped) >= max_candidates:
            break
    return deduped


def _detect_pattern(text: str) -> str | None:
    if (("korean version" in text or "japanese version" in text) and ("is there" in text or "version of it" in text)):
        return "localized_existence"
    if "japanese name" in text and "card" in text:
        return "localized_translation_by_card"
    if "language of the" in text and "set" in text:
        return "language_by_set_name"
    return None


def _extract_quoted_literals(text: str) -> list[str]:
    values = []
    raw = text or ""
    patterns = [
        r'"([^"]{1,200})"',
        r"'((?:''|[^']){1,200})'",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, raw):
            norm = str(match).replace("''", "'").strip()
            if norm:
                values.append(norm)
    return values
