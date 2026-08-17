from __future__ import annotations

import re
import sqlite3
from typing import Any


_DATE_COL_TOKENS = ("date", "time", "year", "month", "day")


def search_date_anchor_hints(
    *,
    db_path: str,
    task_spec: dict[str, Any],
    query_spec: dict[str, Any],
    max_hints: int = 12,
) -> list[dict[str, Any]]:
    literals = _collect_date_like_literals(task_spec, query_spec)
    if not literals:
        return []

    hints: list[dict[str, Any]] = []
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    try:
        for table in _get_tables(conn):
            for column in _get_table_columns(conn, table):
                if not _looks_date_like_column(column):
                    continue
                for literal in literals:
                    metrics = _match_date_literal(conn, table, column, literal)
                    if metrics is None:
                        continue
                    hints.append(
                        {
                            "table": table,
                            "column": column,
                            "literal": literal,
                            **metrics,
                        }
                    )
    finally:
        conn.close()

    hints.sort(
        key=lambda item: (
            -int(item.get("score", 0)),
            -int(item.get("match_count", 0)),
            item.get("table", ""),
            item.get("column", ""),
            item.get("literal", ""),
        )
    )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in hints:
        key = (
            str(item.get("table")),
            str(item.get("column")),
            str(item.get("literal")),
            str(item.get("match_kind")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= max_hints:
            break
    return deduped


def _collect_date_like_literals(task_spec: dict[str, Any], query_spec: dict[str, Any]) -> list[str]:
    values = set()
    for value in task_spec.get("protected_literals") or []:
        text = str(value).strip()
        if _looks_date_like_literal(text):
            values.add(text)
    for predicate in query_spec.get("where_predicates") or []:
        for value in predicate.get("literals") or []:
            text = str(value).strip()
            if _looks_date_like_literal(text):
                values.add(text)
    return sorted(values)


def _looks_date_like_literal(value: str) -> bool:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}%?", value):
        return True
    if re.fullmatch(r"\d{4}-\d{2}%?", value):
        return True
    if re.fullmatch(r"\d{6}%?", value):
        return True
    if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", value):
        return True
    return False


def _looks_date_like_column(column: str) -> bool:
    text = str(column or "").lower()
    return any(token in text for token in _DATE_COL_TOKENS)


def _get_tables(conn: sqlite3.Connection) -> list[str]:
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    except Exception:
        return []
    return [str(row[0]).lower() for row in rows if row and row[0]]


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


def _match_date_literal(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    literal: str,
) -> dict[str, Any] | None:
    variants = _literal_variants(literal)
    best: dict[str, Any] | None = None
    for match_kind, sql_expr, bind_value, score in variants:
        try:
            row = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE {sql_expr.format(col=column)}',
                (bind_value,),
            ).fetchone()
        except Exception:
            continue
        count = int(row[0]) if row and row[0] is not None else 0
        if count <= 0:
            continue
        candidate = {
            "match_kind": match_kind,
            "match_count": count,
            "score": score,
        }
        if best is None or (candidate["score"], candidate["match_count"]) > (
            best["score"],
            best["match_count"],
        ):
            best = candidate
    return best


def _literal_variants(literal: str) -> list[tuple[str, str, str, int]]:
    text = literal.strip()
    variants: list[tuple[str, str, str, int]] = []

    if text.endswith("%"):
        bare = text[:-1]
    else:
        bare = text

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", bare):
        variants.append(("exact_date", 'CAST("{col}" AS TEXT) = ?', bare, 120))
        variants.append(("date_prefix", 'CAST("{col}" AS TEXT) LIKE ?', bare + "%", 110))
        month_key = bare[:4] + bare[5:7]
        variants.append(
            (
                "month_key_from_date",
                'SUBSTR(REPLACE(CAST("{col}" AS TEXT), "-", ""), 1, 6) = ?',
                month_key,
                90,
            )
        )
    elif re.fullmatch(r"\d{6}", bare):
        variants.append(("month_key_exact", 'CAST("{col}" AS TEXT) = ?', bare, 120))
        variants.append(
            (
                "month_key_from_date",
                'SUBSTR(REPLACE(CAST("{col}" AS TEXT), "-", ""), 1, 6) = ?',
                bare,
                110,
            )
        )
        dashed = bare[:4] + "-" + bare[4:6]
        variants.append(("month_prefix_dashed", 'CAST("{col}" AS TEXT) LIKE ?', dashed + "%", 100))
    elif re.fullmatch(r"\d{4}-\d{2}", bare):
        month_key = bare[:4] + bare[5:7]
        variants.append(("month_prefix_dashed", 'CAST("{col}" AS TEXT) LIKE ?', bare + "%", 120))
        variants.append(
            (
                "month_key_from_dashed_prefix",
                'SUBSTR(REPLACE(CAST("{col}" AS TEXT), "-", ""), 1, 6) = ?',
                month_key,
                110,
            )
        )
    elif re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", bare):
        parts = bare.split("/")
        normalized = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        variants.append(("slash_date_normalized", 'CAST("{col}" AS TEXT) = ?', normalized, 120))
        variants.append(("slash_date_prefix", 'CAST("{col}" AS TEXT) LIKE ?', normalized + "%", 110))

    return variants
