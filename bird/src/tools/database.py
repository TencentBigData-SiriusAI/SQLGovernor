"""Database access helpers for schema reflection and mschema generation."""

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from loguru import logger


_INTERNAL_TABLE_PREFIX = "sqlite_"


def _quote_identifier(identifier: str) -> str:
    """Quote a SQLite identifier."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def get_database_schema(database_path: str) -> Dict[str, Any]:
    """Reflect the schema of a SQLite database.

    Args:
        database_path: Path to the SQLite file.

    Returns:
        A dict with:
        - tables: list of table definitions
        - foreign_keys: list of foreign-key relationships
    """
    if not database_path or not Path(database_path).exists():
        raise FileNotFoundError(f"Database not found: {database_path}")

    logger.info(f"Reflecting schema: {database_path}")

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    try:
        # List user tables (skip sqlite_* internals).
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row[0] for row in cursor.fetchall() if not row[0].startswith(_INTERNAL_TABLE_PREFIX)]

        tables = []
        foreign_keys = []

        for table_name in table_names:
            quoted_table = _quote_identifier(table_name)
            # Fetch column info.
            try:
                cursor.execute(f"PRAGMA table_info({quoted_table})")
                columns_info = cursor.fetchall()
            except sqlite3.Error as exc:  # pragma: no cover - pragma failure
                logger.warning("Failed to read table info", table=table_name, error=str(exc))
                columns_info = []

            columns = []
            for col in columns_info:
                columns.append({
                    "name": col[1],
                    "type": col[2],
                    "not_null": bool(col[3]),
                    "default_value": col[4],
                    "is_primary_key": bool(col[5]),
                })

            tables.append({
                "name": table_name,
                "columns": columns,
            })

            # Fetch foreign keys.
            try:
                cursor.execute(f"PRAGMA foreign_key_list({quoted_table})")
                fk_info = cursor.fetchall()
            except sqlite3.Error as exc:  # pragma: no cover - pragma failure
                logger.warning("Failed to read foreign keys", table=table_name, error=str(exc))
                fk_info = []

            for fk in fk_info:
                foreign_keys.append({
                    "from_table": table_name,
                    "from_column": fk[3],
                    "to_table": fk[2],
                    "to_column": fk[4],
                })

        schema_info = {
            "tables": tables,
            "foreign_keys": foreign_keys,
            "table_count": len(tables),
        }

        logger.info(f"Schema reflected: {len(tables)} tables, {len(foreign_keys)} foreign keys")

        return schema_info

    finally:
        conn.close()


import re

def iif_to_case_nested(sql: str) -> str:

    def _split_iif_args(content: str):
        """Split IIF arguments on commas, respecting quotes and parentheses."""
        args = []
        current = ""
        in_single_quote = False
        in_double_quote = False
        paren_depth = 0
        i = 0
        while i < len(content):
            c = content[i]
            if c == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif c == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif c == '(' and not in_single_quote and not in_double_quote:
                paren_depth += 1
            elif c == ')' and not in_single_quote and not in_double_quote:
                paren_depth -= 1
            elif c == ',' and paren_depth == 0 and not in_single_quote and not in_double_quote:
                args.append(current.strip())
                current = ""
                i += 1
                continue

            current += c
            i += 1
        if current.strip():
            args.append(current.strip())
        return args

    """Rewrite SQL Server `IIF(...)` into `CASE WHEN ... END`, handling nesting."""
    # Strategy: repeatedly rewrite the innermost IIF (no nested IIF inside).
    # Matching rules:
    # - `\bIIF\b` matches the keyword.
    # - `\(` matches the opening parenthesis (balanced matching).

    def replace_innermost_iif(text):
        # Locate all IIF occurrences.
        iif_pattern = re.compile(r'\bIIF\s*\(', re.IGNORECASE)
        matches = list(iif_pattern.finditer(text))
        if not matches:
            return text, False

        # Scan left-to-right, tracking balanced parentheses.
        # Nested IIF: skip the outer one and continue after it.
        pos = 0
        while pos < len(text):
            match = iif_pattern.search(text, pos)
            if not match:
                break
            start = match.start()
            # Find the matching closing paren of this IIF.
            # The opening paren is the char just before match.end().
            paren_start = match.end() - 1  # position of '('
            paren_count = 1
            i = paren_start + 1
            while i < len(text) and paren_count > 0:
                if text[i] == '(':
                    paren_count += 1
                elif text[i] == ')':
                    paren_count -= 1
                i += 1
            if paren_count != 0:
                raise ValueError("Unmatched parentheses in SQL")

            iif_full_end = i  # position after the closing ')'
            iif_content = text[paren_start+1:iif_full_end-1]  # inner content

            # If the content contains another IIF, handle the inner one first.
            if re.search(r'\bIIF\b', iif_content, re.IGNORECASE):
                # Skip the outer IIF and try the inner one.
                pos = match.end()
                continue

            # Found an innermost IIF: split its arguments.
            args = _split_iif_args(iif_content)
            if len(args) != 3:
                raise ValueError(f"Invalid IIF argument count: {iif_content}")

            cond, true_val, false_val = args
            case_expr = f"(CASE WHEN {cond} THEN {true_val} ELSE {false_val} END)"
            new_text = text[:start] + case_expr + text[iif_full_end:]
            return new_text, True

        return text, False

    # Iteratively rewrite until no IIF remains.
    current = sql
    while True:
        current, replaced = replace_innermost_iif(current)
        if not replaced:
            break
    return current


def execute_sql(sql: str, database_path: str) -> List[Any]:
    """Execute a SQL query and return the rows.

    Args:
        sql: The SQL to execute.
        database_path: Path to the SQLite file.

    Returns:
        The fetched rows.

    Raises:
        sqlite3.Error: If execution fails.
    """
    if not database_path or not Path(database_path).exists():
        raise FileNotFoundError(f"Database not found: {database_path}")

    # logger.info(f"Executing SQL: {sql[:100]}...")

    if "iif" in sql.lower():
        sql = iif_to_case_nested(sql)

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        results = cursor.fetchall()

        # logger.info(f"SQL executed, {len(results)} rows")

        return results

    except sqlite3.Error as e:
        logger.error(f"SQL execution failed: {e}")
        raise e

    finally:
        conn.close()


def get_table_names(database_path: str) -> List[str]:
    """Return the list of table names in a database.

    Args:
        database_path: Path to the SQLite file.

    Returns:
        List of table names.
    """
    schema = get_database_schema(database_path)
    return [table["name"] for table in schema["tables"]]


def get_table_columns(database_path: str, table_name: str) -> List[str]:
    """Return column names for a table."""

    schema = get_database_schema(database_path)
    for table in schema["tables"]:
        if table["name"] == table_name:
            return [col["name"] for col in table["columns"]]
    return []


def fetch_distinct_values(
    database_path: str, table_name: str, column_name: str, max_num: int = 5
) -> List[Any]:
    """Fetch distinct values for a column."""

    query_sql = f'SELECT DISTINCT "{column_name}" FROM "{table_name}" LIMIT ?'
    values: List[Any] = []
    try:
        with sqlite3.connect(database_path) as connection:
            cursor = connection.cursor()
            cursor.execute(query_sql, (max_num,))
            for value_tuple in cursor.fetchall():
                value = value_tuple[0]
                if value not in (None, ""):
                    values.append(value)
    except sqlite3.Error as exc:  # pragma: no cover - query failure
        logger.debug(
            f"Failed to fetch values table={table_name} column={column_name}: {exc}"
        )
    return values


def load_extra_schema_info(database_path: str, schema_info: Dict[str, Any], table_file_path: str) -> Dict[str, Any]:
    """Enrich schema info with descriptions and examples for mschema."""

    enriched = schema_info
    enriched["db_id"] = Path(database_path).stem

    tables_path = Path(table_file_path)

    sample: Dict[str, Any] | None = None
    if tables_path.exists():
        try:
            with tables_path.open("r", encoding="utf-8") as handle:
                records = json.load(handle)
            for item in records:
                if item.get("db_id") == enriched["db_id"]:
                    sample = item
                    break

            table_name_mappings = {
                original: mapped
                for original, mapped in zip(
                    sample.get("table_names_original", []), sample.get("table_names", [])
                )
            }
        except Exception as exc:  # pragma: no cover - load failure
            logger.warning(f"Failed to load {tables_path}: {exc}")
    if not sample:
        logger.warning(
            f"No table info for {enriched['db_id']}"
        )
        table_name_mappings = {}
        # return enriched

    description_dir = Path(database_path).parent / "database_description"

    for table in enriched.get("tables", []):
        columns = table.get("columns", [])
        for column in columns:
            column_name = column.get("name")
            column["examples"] = fetch_distinct_values(
                database_path, table.get("name"), column_name, 5
            )

        mapped_name = table_name_mappings.get(table.get("name"), table.get("name"))
        csv_path = description_dir / f"{mapped_name}.csv"
        if not csv_path.exists():
            csv_path = description_dir / f"{table.get('name')}.csv"
        if not csv_path.exists():
            continue

        try:
            df = pd.read_csv(csv_path, encoding="utf-8", encoding_errors="ignore")
        except Exception as exc:  # pragma: no cover - csv failure
            logger.warning(f"Failed to read CSV: {exc}")
            continue
        df = df.fillna("")
        field_extra_info: Dict[str, Dict[str, str]] = {}
        for _, row in df.iterrows():
            original_name = row.get("original_column_name")
            if not original_name:
                continue
            field_extra_info[original_name] = {
                "column_description": row.get("column_description", ""),
                "value_description": row.get("value_description", ""),
                "column_name": row.get("column_name", ""),
            }

        for column in columns:
            name = column.get("name")
            extra = field_extra_info.get(name, {})
            column["value_description"] = extra.get("value_description", "").replace("\n", "; ")
            column["column_description"] = extra.get("column_description", "").replace("\n", "; ")

    return enriched


def format_mschema(schema_info: Dict[str, Any]) -> str:
    parts: List[str] = [f"【DB_ID】 {schema_info.get('db_id', '')}", "【Schema】"]
    for table in schema_info.get("tables", []):
        parts.append(f"# Table: {table.get('name')}")
        parts.append("[")
        column_lines = []
        for column in table.get("columns", []):
            column_line = f"({column.get('name')}:{column.get('type')})"
            flags = []
            if column.get("is_primary_key"):
                flags.append("PRIMARY KEY")
            examples = column.get("examples") or []
            if examples:
                if len(str(examples)) < 150:
                    flags.append("EXAMPLES: " + str(examples))
                else:
                    logger.warning(f"Examples too long for {column.get('name')}: {examples}")
            alias = column.get("column_name")
            if alias and alias != column.get("name"):
                flags.append(f"ALIAS: {alias}")
            if column.get("column_description"):
                flags.append(f"COLUMN_DESCRIPTION: {column['column_description']}")
            if column.get("value_description"):
                flags.append(f"VALUE_DESCRIPTION: {column['value_description']}")
            if flags:
                column_line = column_line[:-1] + ", " + ", ".join(flags) + ")"
            column_lines.append(column_line)
        parts.append(",\n".join(column_lines))
        parts.append("]")

    parts.append("【Foreign keys】")
    for fk in schema_info.get("foreign_keys", []):
        parts.append(
            f"{fk.get('from_table')}.{fk.get('from_column')}={fk.get('to_table')}.{fk.get('to_column')}"
        )
    return "\n".join(parts) + "\n"


def generate_mschema_str(database_path: str, table_file_path: str) -> str:
    """Generate an XMschema string for a SQLite database."""

    schema_info = get_database_schema(database_path)
    schema_info = load_extra_schema_info(database_path, schema_info, table_file_path)
    return format_mschema(schema_info)
