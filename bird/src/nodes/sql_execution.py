"""Execute the current SQL candidate against the target database."""

from typing import Dict, Any, List, Optional
from loguru import logger

from ..core.state import SQLAgentState
from ..tools.database import execute_sql


def sql_execution_node(state: SQLAgentState) -> Dict[str, Any]:
    """Run the current candidate SQL and record the execution outcome.

    Steps:
    1. Fetch the candidate SQL.
    2. Execute it against the database.
    3. Record the result or error.
    """
    logger.debug("=" * 60)
    logger.debug("Executing SQL candidate")
    logger.debug("=" * 60)

    candidate_sql = state.get("candidate_sql", "")
    database_path = state.get("database_path", "")

    if not candidate_sql:
        logger.error("No candidate SQL to execute")
        return {
            "execution_result": {
                "success": False,
                "result": None,
                "error": "No candidate SQL",
            },
        }

    logger.debug(f"Executing SQL:\n{candidate_sql}")

    try:
        # Execute the candidate SQL against the database.
        result = execute_sql(candidate_sql, database_path)

        logger.debug(f"SQL execution succeeded with {len(result) if result else 0} rows")

        # Preview the first few rows for debugging.
        if result:
            logger.debug(f"Result preview: {result[:3]}")

        exec_payload = {
            "success": True,
            "result": result,
            "error": None,
            "row_count": len(result) if result else 0,
        }
        attempted_update = _update_attempt_log(state, exec_payload)
        execution_snapshots = _update_execution_snapshots(state, exec_payload)

        return {
            "execution_result": exec_payload,
            "attempted_candidates": attempted_update,
            "candidate_execution_results": execution_snapshots,
        }

    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        exec_payload = {
            "success": False,
            "result": None,
            "error": str(e),
        }
        attempted_update = _update_attempt_log(state, exec_payload)
        execution_snapshots = _update_execution_snapshots(state, exec_payload)
        return {
            "execution_result": exec_payload,
            "attempted_candidates": attempted_update,
            "candidate_execution_results": execution_snapshots,
            "error_message": f"SQL execution failed: {e}",
        }


def _update_attempt_log(
    state: SQLAgentState, execution_payload: Dict[str, Any]
) -> List[Dict[str, Any]]:
    attempted = list(state.get("attempted_candidates", []))
    attempt_idx = state.get("current_attempt_index")
    if attempt_idx is None:
        return attempted
    if 0 <= attempt_idx < len(attempted):
        attempt_entry = dict(attempted[attempt_idx])
        attempt_entry["execution"] = execution_payload
        attempted[attempt_idx] = attempt_entry
    return attempted


def _update_execution_snapshots(
    state: SQLAgentState, execution_payload: Dict[str, Any]
) -> List[Optional[Dict[str, Any]]]:
    """Align execution payloads with `generation_candidates` indices."""

    execution_results: List[Optional[Dict[str, Any]]] = list(
        state.get("candidate_execution_results", [])
    )
    candidate_count = len(state.get("generation_candidates", []))
    if len(execution_results) < candidate_count:
        execution_results.extend([None] * (candidate_count - len(execution_results)))

    candidate_idx = state.get("current_candidate_index")
    if candidate_idx is not None:
        if candidate_idx >= len(execution_results):
            execution_results.extend([None] * (candidate_idx + 1 - len(execution_results)))
        execution_results[candidate_idx] = execution_payload
    return execution_results
