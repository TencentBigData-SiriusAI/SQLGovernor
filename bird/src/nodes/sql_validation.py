"""Validate the current SQL candidate for syntax and schema correctness."""

from typing import Dict, Any, List
from loguru import logger

from ..core.state import SQLAgentState
from ..tools.sql_validator import validate_sql_syntax, validate_sql_schema


def sql_validation_node(state: SQLAgentState) -> Dict[str, Any]:
    """Validate the candidate SQL.

    Steps:
    1. Syntax check: validate SQL syntax.
    2. Schema check: validate tables and columns.
    3. Logic check: emit warnings for potential logic issues.
    """
    logger.debug("=" * 60)
    logger.debug("Validating SQL candidate")
    logger.debug("=" * 60)

    candidate_sql = state.get("candidate_sql", "")
    schema_info = state.get("schema_info", {})
    database_path = state.get("database_path", "")

    if not candidate_sql:
        logger.error("No candidate SQL to validate")
        return {
            "validation_result": {
                "is_valid": False,
                "errors": ["No candidate SQL"],
                "warnings": [],
            },
        }

    logger.debug(f"Validating SQL:\n{candidate_sql}")

    errors: List[str] = []
    warnings: List[str] = []

    # 1. Syntax check
    syntax_errors = validate_sql_syntax(candidate_sql)
    if syntax_errors:
        logger.warning(f"Syntax errors: {syntax_errors}")
        errors.extend(syntax_errors)
    else:
        logger.debug("Syntax check passed")

    # 2. Schema check
    schema_errors = validate_sql_schema(candidate_sql, schema_info, database_path)
    if schema_errors:
        logger.warning(f"Schema errors: {schema_errors}")
        errors.extend(schema_errors)
    else:
        logger.debug("Schema check passed")

    # 3. Logic check (warnings only)
    logic_warnings = check_sql_logic(candidate_sql)
    if logic_warnings:
        logger.warning(f"Logic warnings: {logic_warnings}")
        warnings.extend(logic_warnings)

    # Validation passes only if there are no hard errors.
    is_valid = len(errors) == 0

    if is_valid:
        logger.debug("SQL validation passed")
    else:
        logger.error(f"SQL validation failed with {len(errors)} errors")

    attempt_log = _record_validation_attempt(
        state,
        candidate_sql,
        {
            "is_valid": is_valid,
            "errors": list(errors),
            "warnings": list(warnings),
        },
    )

    return {
        "validation_result": {
            "is_valid": is_valid,
            "errors": errors,
            "warnings": warnings,
        },
        "attempted_candidates": attempt_log["attempted_candidates"],
        "current_attempt_index": attempt_log["current_attempt_index"],
    }


def check_sql_logic(sql: str) -> List[str]:
    """Emit non-fatal warnings for potential SQL logic issues.

    Currently checks:
    - Use of SELECT *
    - Missing WHERE clause on a SELECT
    """
    warnings = []

    sql_upper = sql.upper()

    # Warn on SELECT *
    if "SELECT *" in sql_upper:
        warnings.append("Avoid SELECT *; list columns explicitly.")

    # Warn on SELECT without WHERE or JOIN
    if "SELECT" in sql_upper and "WHERE" not in sql_upper and "JOIN" not in sql_upper:
        warnings.append("SELECT has no WHERE or JOIN clause.")

    return warnings


def _record_validation_attempt(
    state: SQLAgentState,
    candidate_sql: str,
    validation_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Append/prepare attempt metadata for downstream execution logging."""

    attempted = list(state.get("attempted_candidates", []))
    candidate_meta = state.get("current_candidate_meta", {})

    attempt_entry = {
        "index": candidate_meta.get("index"),
        "queue_index": state.get("current_candidate_index"),
        "sql": candidate_sql,
        "vote_count": candidate_meta.get("vote_count", 0),
        "source": candidate_meta.get("source", "generation"),
        "validation": validation_payload,
        "execution": None,
    }

    attempted.append(attempt_entry)
    current_attempt_index = len(attempted) - 1

    return {
        "attempted_candidates": attempted,
        "current_attempt_index": current_attempt_index,
    }
