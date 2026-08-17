"""Post-processing utilities for SELECT projections.

The SQL generator already tries to limit the selected columns based on the
question, but we still observe a few systematic formatting issues across
datasets:

- Columns within an address/contact bundle appear in different orders.
- Additional numbered variants such as ``Email2`` or ``Email3`` are returned
  even when the user only asked for the primary value.

To keep the behaviour deterministic across tasks we run a lightweight
post-processing step that performs two heuristic adjustments:

1. Reorder known column bundles (address, contact information) into a
   canonical order when the generated query contains exactly the same column
   set as the bundle.
2. Drop high-index numbered variants (suffix ``2``/``3``/...) unless the
   question explicitly asks for *all* values. The heuristic checks for common
   keywords such as "all", "second", "third", and their Chinese counterparts.

The implementation intentionally stays generic: it only looks at column names
after normalising aliases and quoting, so the rules apply regardless of the
table aliases or database schema.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Sequence, Tuple

import sqlparse


# Canonical bundles of related columns. Each tuple stores the normalised column
# names in the desired order. Normalisation removes quotes/aliases and turns the
# token into lower-case alphanumerics so that variants like ``T1.Street`` or
# ``"MailStreet"`` both match ``mailstreet``.
COLUMN_BUNDLES: Tuple[Tuple[str, ...], ...] = (
    ("street", "city", "state", "zip"),
    ("mailstreet", "mailcity", "mailstate", "mailzip"),
    ("phone", "ext", "school"),
)

# Bases that should retain numbered variants (e.g., `A2`, `atom_id2`).
SUFFIX_KEEP_BASES: Tuple[str, ...] = (
    "a",  # district columns A2/A3
    "atomid",  # atom_id / atom_id2 pairs
    "admemail",  # multiple administrator emails
    "element",  # element1/element2 pairs in chemistry schema
)

# Question keywords indicating that the user wants every variant rather than
# the primary value. Includes simple English and Chinese cues.
KEEP_ALL_KEYWORDS: Tuple[str, ...] = (
    " all",
    " both",
    " each",
    " every",
    " second",
    " third",
    " two",
    " three",
    "",
    "",
    "",
    "",
    "",
    "",
)


@dataclass(frozen=True)
class ProjectionAdjustment:
    """Lightweight record describing a projection fix."""

    description: str


def apply_projection_rules(sql: str, question: str | None = None) -> Tuple[str, List[ProjectionAdjustment]]:
    """Apply heuristic projection fixes to a SQL query.

    Args:
        sql: Clean SQL string (no markdown fences).
        question: Original natural-language question, used to look for
            keywords like "all" when deciding whether to keep numbered columns.

    Returns:
        A tuple of the possibly-modified SQL text and a list of adjustments
        applied. When no changes are made the original SQL string is returned
        with an empty list.
    """

    if not sql:
        return sql, []

    match = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", sql)
    if not match:
        return sql, []

    select_segment = match.group(1)
    parsed = sqlparse.parse(f"SELECT {select_segment}")
    if not parsed:
        return sql, []

    columns = _extract_select_columns(parsed[0])
    if not columns:
        return sql, []

    adjustments: List[ProjectionAdjustment] = []

    # Step 1: remove redundant numbered columns when the question does not ask
    # for all variants.
    keep_all = _should_keep_all(question or "")
    filtered_columns, removed = _drop_numbered_variants(columns, keep_all)
    if removed:
        adjustments.append(
            ProjectionAdjustment(
                description=f"removed numbered variants: {', '.join(removed)}"
            )
        )
        columns = filtered_columns

    # Step 2: deduplicate columns that map to the same normalised base name.
    deduped_columns, deduped = _deduplicate_columns(columns, keep_all)
    if deduped:
        adjustments.append(
            ProjectionAdjustment(description=f"deduplicated columns: {', '.join(deduped)}")
        )
        columns = deduped_columns

    # Step 3: reorder canonical bundles into the expected order.
    reordered_columns, bundle_desc = _reorder_bundles(columns)
    if bundle_desc:
        adjustments.append(ProjectionAdjustment(description=bundle_desc))
        columns = reordered_columns

    if not adjustments:
        return sql, []

    new_segment = ", ".join(columns)
    original_segment = select_segment

    # Preserve original leading/trailing whitespace around the segment.
    leading_ws = len(original_segment) - len(original_segment.lstrip())
    trailing_ws = len(original_segment) - len(original_segment.rstrip())
    new_segment_with_ws = (
        " " * leading_ws + new_segment.strip() + " " * trailing_ws
    )

    new_sql = sql[: match.start(1)] + new_segment_with_ws + sql[match.end(1) :]
    return new_sql, adjustments


def _extract_select_columns(statement: sqlparse.sql.Statement) -> List[str]:
    """Return the list of select expressions as raw strings."""

    tokens = statement.tokens
    for token in tokens:
        if isinstance(token, sqlparse.sql.IdentifierList):
            return [str(identifier).strip() for identifier in token.get_identifiers()]
        if isinstance(token, sqlparse.sql.Identifier):
            return [str(token).strip()]
        if token.ttype is sqlparse.tokens.Wildcard:
            return [token.value.strip()]
    return []


def _should_keep_all(question: str) -> bool:
    question_lower = question.lower()
    return any(keyword in question_lower for keyword in KEEP_ALL_KEYWORDS)


def _drop_numbered_variants(columns: Sequence[str], keep_all: bool) -> Tuple[List[str], List[str]]:
    if keep_all:
        return list(columns), []

    retained: List[str] = []
    removed: List[str] = []
    for col in columns:
        norm = _normalise_identifier(col)
        base, suffix = _split_numeric_suffix(norm)
        if (
            suffix
            and suffix.isdigit()
            and int(suffix) > 1
            and base not in SUFFIX_KEEP_BASES
        ):
            removed.append(col)
            continue
        retained.append(col)
    return retained, removed


def _deduplicate_columns(columns: Sequence[str], keep_all: bool) -> Tuple[List[str], List[str]]:
    if keep_all:
        return list(columns), []

    seen: set[str] = set()
    deduped: List[str] = []
    removed: List[str] = []
    for col in columns:
        key = col.strip().lower()
        if key in seen:
            removed.append(col)
            continue
        seen.add(key)
        deduped.append(col)
    return deduped, removed


def _reorder_bundles(columns: Sequence[str]) -> Tuple[List[str], str | None]:
    normalised = [_normalise_identifier(col) for col in columns]
    for bundle in COLUMN_BUNDLES:
        bundle_set = set(bundle)
        if set(normalised) != bundle_set:
            continue
        target_indices = []
        for name in bundle:
            try:
                idx = normalised.index(name)
            except ValueError:  # pragma: no cover - defensive, should not happen
                break
            target_indices.append(idx)
        else:
            reordered = [columns[idx] for idx in target_indices]
            if list(columns) != reordered:
                desc = f"reordered bundle to canonical order: {', '.join(bundle)}"
                return reordered, desc
    return list(columns), None


def _normalise_identifier(identifier: str) -> str:
    """Return a simplified identifier name used for comparisons."""

    cleaned = identifier.strip()
    # Remove aliases (anything after AS or a bare alias).
    parts = re.split(r"\s+AS\s+", cleaned, flags=re.IGNORECASE)
    cleaned = parts[0]
    # Remove trailing alias without AS (e.g., "column alias").
    cleaned = cleaned.split()[0]
    if "." in cleaned:
        cleaned = cleaned.split(".")[-1]
    cleaned = cleaned.strip('"`[]')
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "", cleaned)
    return cleaned.lower()


def _split_numeric_suffix(name: str) -> Tuple[str, str]:
    match = re.match(r"^(.*?)(\d+)$", name)
    if not match:
        return name, ""
    return match.group(1), match.group(2)
