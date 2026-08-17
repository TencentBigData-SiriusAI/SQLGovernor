"""Confidence-gated prompt injection.

Converts retrieval results into prompt sections based on confidence level:
  WARN:    high confidence + exact layer → inject DB facts (actual values)
  HINT:    medium confidence             → inject error description
  EXAMPLE: low confidence + semantic     → inject similar case as few-shot
"""
from __future__ import annotations

from typing import List

from error_bank.retriever import RetrievalResult
from error_bank.confidence import THRESHOLD_WARN, THRESHOLD_HINT, THRESHOLD_EXAMPLE


def build_error_context(
    results: List[RetrievalResult],
    max_warns: int = 3,
    max_hints: int = 3,
    max_examples: int = 2,
) -> str:
    """Build error context string for prompt injection.

    Applies confidence gating:
      Layer 1-2 + conf ≥ WARN   → WARN section (DB facts)
      Layer 2-3 + conf ≥ HINT   → HINT section (error description)
      Layer 3-4 + conf ≥ EXAMPLE → EXAMPLE section (similar case)

    Returns empty string if no results pass gating.
    """
    warns = []
    hints = []
    examples = []

    for r in results:
        conf = r.confidence
        layer = r.layer
        e = r.entry

        # WARN: high confidence + precise layer → inject actual DB values
        if layer <= 2 and conf >= THRESHOLD_WARN and e.db_facts:
            if len(warns) < max_warns:
                lines = []
                for fact in e.db_facts:
                    lines.append(f"  Column {fact.column}:")
                    if fact.actual_values:
                        lines.append(f"    Actual values in DB: {fact.actual_values[:8]}")
                    if fact.expected_value:
                        lines.append(f"    SQL used: '{fact.expected_value}'")
                    if fact.closest_match:
                        lines.append(f"    Closest match: '{fact.closest_match}'")
                    if fact.suggested_fix:
                        lines.append(f"    Suggested: {fact.suggested_fix}")
                if lines:
                    warns.append("\n".join(lines))

        # HINT: medium confidence → inject error description as warning
        elif conf >= THRESHOLD_HINT:
            if len(hints) < max_hints:
                hint = f"  [{e.error_type.value}] {e.error_detail[:150]}"
                if e.killer_condition:
                    hint += f"\n    Killer condition: {e.killer_condition[:100]}"
                if e.fix_succeeded and e.fix_sql:
                    hint += f"\n    Previously fixed by: {e.fix_sql[:100]}"
                hints.append(hint)

        # EXAMPLE: low confidence / semantic layer → similar case
        elif layer >= 3 and conf >= THRESHOLD_EXAMPLE:
            if len(examples) < max_examples:
                ex = f"  Similar case (QID {e.question_id}):"
                ex += f"\n    Question: {e.question[:100]}"
                ex += f"\n    Error: {e.error_type.value} — {e.error_detail[:80]}"
                if e.fix_succeeded and e.fix_sql:
                    ex += f"\n    Fixed SQL: {e.fix_sql[:150]}"
                examples.append(ex)

    # Assemble sections
    sections = []

    if warns:
        sections.append("⚠️ KNOWN DATABASE FACTS (verified by querying actual data):")
        sections.extend(warns)

    if hints:
        sections.append("\n📋 KNOWN ERROR PATTERNS FOR THIS DATABASE:")
        sections.extend(hints)

    if examples:
        sections.append("\n📎 SIMILAR CASES (for reference):")
        sections.extend(examples)

    return "\n".join(sections) if sections else ""
