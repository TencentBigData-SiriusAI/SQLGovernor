"""Validate SQL syntax and schema correctness."""

import re
import sqlite3
from typing import List, Dict, Any
from loguru import logger


def validate_sql_syntax(sql: str) -> List[str]:
    """Check basic SQL syntax and return a list of errors.

    Args:
        sql: The SQL string to validate.

    Returns:
        A list of syntax error messages (empty if valid).
    """
    errors = []

    if not sql or not sql.strip():
        errors.append("SQL is empty")
        return errors

    sql_upper = sql.upper().strip()

    # Must start with SELECT or WITH (read-only queries only).
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        errors.append("SQL must start with SELECT or WITH")

    # A SELECT must reference a FROM clause.
    if "SELECT" in sql_upper:
        if "FROM" not in sql_upper:
            errors.append("SELECT statement is missing FROM")

    # Check balanced parentheses (ignoring string literals).
    sanitized = re.sub(r"'([^']|'')*'", "", sql)
    sanitized = re.sub(r'"([^"]|"")*"', "", sanitized)
    if sanitized.count("(") != sanitized.count(")"):
        errors.append("Unbalanced parentheses")

    # Check balanced single quotes.
    single_quotes = sql.count("'")
    if single_quotes % 2 != 0:
        errors.append("Unbalanced single quotes")

    # Check balanced double quotes.
    double_quotes = sql.count('"')
    if double_quotes % 2 != 0:
        errors.append("Unbalanced double quotes")

    # Check for duplicate commas.
    if ",," in sql:
        errors.append("Duplicate commas")

    # Check for trailing comma.
    if sql.rstrip().endswith(","):
        errors.append("SQL ends with a trailing comma")

    return errors


def validate_sql_schema(
    sql: str,
    schema_info: Dict[str, Any],
    database_path: str = None
) -> List[str]:
    """Validate SQL against the provided schema.

    Checks:
    1. Referenced tables exist.
    2. Referenced columns exist.
    3. EXPLAIN succeeds against the database.

    Args:
        sql: The SQL string to validate.
        schema_info: Schema information (tables and columns).
        database_path: Optional database path for EXPLAIN validation.

    Returns:
        A list of schema error messages (empty if valid).
    """
    errors = []

    schema_available = bool(schema_info) and "tables" in schema_info
    if not schema_available:
        logger.warning("Schema unavailable; falling back to EXPLAIN validation")
        return _validate_with_explain(sql, database_path)

    # Collect valid tables and columns.
    valid_tables = {table["name"].lower() for table in schema_info["tables"]}
    table_columns = {}

    for table in schema_info["tables"]:
        table_name = table["name"].lower()
        table_columns[table_name] = {col["name"].lower() for col in table["columns"]}

    # Extract CTE names so they are not treated as tables.
    cte_names = extract_cte_names(sql)

    # Extract referenced tables from the SQL.
    tables_in_sql = extract_table_names(sql)

    for table in tables_in_sql:
        if table.lower() in cte_names:
            continue  # CTE reference, not a base table
        if table.lower() not in valid_tables:
            errors.append(f"Table '{table}' not found in schema")

    explain_errors: List[str] = []
    if database_path:
        explain_errors = _validate_with_explain(sql, database_path)
        if not explain_errors:
            # EXPLAIN succeeded, so table-not-found errors are CTE/alias artifacts.
            errors = [err for err in errors if not err.startswith("Table '")]

    errors.extend(explain_errors)

    return errors


def _validate_with_explain(sql: str, database_path: str) -> List[str]:
    """Use SQLite EXPLAIN to validate SQL against the actual schema.

    Opens the database read-only (immutable=1) with WAL disabled.
    """
    errors: List[str] = []
    if not database_path:
        logger.warning("No database path; skipping EXPLAIN validation")
        return errors

    conn = None
    try:
        # immutable=1: open read-only without locking.
        # EXPLAIN validates tables/columns without running the query.
        uri = f"file:{database_path}?mode=ro&immutable=1"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute(f"EXPLAIN {sql}")
        cursor.fetchall()
        logger.debug("Schema check passed (EXPLAIN)")
    except (sqlite3.Error, sqlite3.Warning) as exc:
        error_msg = str(exc)
        if "no such table" in error_msg:
            errors.append(f"Table not found: {error_msg}")
        elif "no such column" in error_msg:
            errors.append(f"Column not found: {error_msg}")
        elif "ambiguous column" in error_msg:
            errors.append(f"Ambiguous column: {error_msg}")
        elif "execute one statement" in error_msg:
            errors.append(f"Multiple statements: {error_msg}")
        else:
            errors.append(f"Schema error: {error_msg}")
    except Exception as e:
        errors.append(f"Validation error: {e}")
    finally:
        if conn is not None:
            conn.close()
    return errors


def extract_cte_names(sql: str) -> set:
    """Extract CTE (common table expression) names from the SQL.

    Example: `WITH foo AS (...), bar AS (...) SELECT ...` -> {'foo', 'bar'}

    Args:
        sql: The SQL string.

    Returns:
        A set of CTE names (lowercase).
    """
    # Matches `WITH name AS (` or `, name AS (`
    pattern = r'(?:WITH|,)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\('
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return {name.lower() for name in matches}


def extract_table_names(sql: str) -> List[str]:
    """Extract base table names from the SQL.

    Returns table names referenced after FROM or JOIN.

    Args:
        sql: The SQL string.

    Returns:
        A list of unique table names.
    """
    table_names = []

    def _strip_string_literals(text: str) -> str:
        return re.sub(r"'([^']|'')*'", "''", text)

    sanitized_sql = _strip_string_literals(sql)

    # Matches FROM table_name
    from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    from_matches = re.findall(from_pattern, sanitized_sql, re.IGNORECASE)
    table_names.extend(from_matches)

    # Matches JOIN table_name
    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    join_matches = re.findall(join_pattern, sanitized_sql, re.IGNORECASE)
    table_names.extend(join_matches)

    # Deduplicate.
    return list(set(table_names))
