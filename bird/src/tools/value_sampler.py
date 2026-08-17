"""Sample distinct column values from a SQLite database."""

from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List

from loguru import logger


def _quote(identifier: str) -> str:
    """Quote a SQLite identifier with backticks."""
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def sample_column_values(
    database_path: str,
    columns: Iterable[str],
    *,
    limit: int = 6,
    truncate_length: int = 40,
) -> Dict[str, List[str]]:
    """Sample distinct values for the given columns."""
    samples: Dict[str, List[str]] = {}

    if not columns:
        return samples

    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
    except sqlite3.Error as exc:  # pragma: no cover - db open failure
        logger.warning(f"Failed to open database: {exc}")
        return samples

    try:
        for column_key in columns:
            if "." not in column_key:
                continue
            table_name, column_name = column_key.split(".", 1)
            table_quoted = _quote(table_name)
            column_quoted = _quote(column_name)
            query = f"""
                SELECT {column_quoted}
                FROM (
                    SELECT DISTINCT {column_quoted}
                    FROM {table_quoted}
                    WHERE {column_quoted} IS NOT NULL
                      AND CAST({column_quoted} AS TEXT) <> ''
                ) AS unique_values
                LIMIT {limit};
            """
            try:
                cursor.execute(query)
            except sqlite3.Error as exc:  # pragma: no cover - query failure
                logger.debug(
                    "Column sampling failed",
                    table=table_name,
                    column=column_name,
                    error=str(exc),
                )
                continue
            fetched = [row[0] for row in cursor.fetchall()]
            normalized: List[str] = []
            for value in fetched:
                if isinstance(value, str) and len(value) > truncate_length:
                    normalized.append(value[:truncate_length])
                else:
                    normalized.append(value)
            if normalized:
                samples[column_key.lower()] = normalized
    finally:
        conn.close()

    return samples


__all__ = ["sample_column_values"]
