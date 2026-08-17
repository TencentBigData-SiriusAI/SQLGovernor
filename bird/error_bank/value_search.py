from __future__ import annotations

import sqlite3
import time
from typing import Any


def search_literal_in_query_tables(
    *,
    db_path: str,
    query_spec: dict[str, Any],
    task_spec: dict[str, Any],
    current_column: str,
    expected_literal: str,
    timeout_seconds: int = 5,
) -> list[dict[str, Any]]:
    current_table, current_col_name = _split_column(current_column)
    query_tables = list(
        dict.fromkeys(
            [str(t).lower() for t in query_spec.get("base_tables") or []]
            + [str(j.get("table") or "").lower() for j in query_spec.get("joins") or []]
        )
    )
    requested_fields = {str(x).lower() for x in task_spec.get("requested_fields") or []}

    candidates: list[dict[str, Any]] = []
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    start_time = time.perf_counter()
    try:
        for table in query_tables:
            if timeout_seconds > 0 and time.perf_counter() - start_time > timeout_seconds:
                break
            if not table:
                continue
            for column in _get_table_columns(conn, table):
                if timeout_seconds > 0 and time.perf_counter() - start_time > timeout_seconds:
                    break
                if table == current_table and column == current_col_name:
                    continue
                exact_matches = _count_exact_matches(
                    conn,
                    table,
                    column,
                    expected_literal,
                    start_time=start_time,
                    timeout_seconds=timeout_seconds,
                )
                if exact_matches <= 0:
                    continue
                score = 0
                if table == current_table:
                    score += 100
                if column == current_col_name:
                    score += 60
                if any(token in column for token in requested_fields):
                    score += 30
                if any(token in table for token in requested_fields):
                    score += 10
                candidates.append(
                    {
                        "table": table,
                        "column": column,
                        "exact_matches": exact_matches,
                        "score": score,
                    }
                )
    finally:
        conn.close()
    candidates.sort(
        key=lambda item: (-item["score"], -item["exact_matches"], item["table"], item["column"])
    )
    return candidates


def _get_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    try:
        rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    except Exception:
        return []
    cols = []
    for row in rows:
        name = row[1] if len(row) > 1 else None
        if name:
            cols.append(str(name).lower())
    return cols


def _count_exact_matches(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    value: str,
    *,
    start_time: float | None = None,
    timeout_seconds: int = 0,
) -> int:
    try:
        if timeout_seconds > 0:
            base = start_time if start_time is not None else time.perf_counter()

            def _progress_handler() -> int:
                if time.perf_counter() - base > timeout_seconds:
                    raise TimeoutError("value_search_timeout")
                return 0

            conn.set_progress_handler(_progress_handler, 1000)
        row = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE CAST("{column}" AS TEXT) = ?',
            (value,),
        ).fetchone()
    except Exception:
        return 0
    finally:
        try:
            conn.set_progress_handler(None, 0)
        except Exception:
            pass
    try:
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _split_column(column: str) -> tuple[str, str]:
    normalized = str(column or "").strip().strip("[]`\"").lower()
    if "." not in normalized:
        return "", normalized
    table, col = normalized.split(".", 1)
    return table, col
