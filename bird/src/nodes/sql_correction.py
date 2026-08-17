"""Correct the current SQL candidate based on validation feedback."""

from typing import Dict, Any
from loguru import logger
from langchain_core.messages import HumanMessage, AIMessage

from ..core.state import SQLAgentState
from config import Settings
from ..tools.sql_correction_helper import generate_corrected_sql


def sql_correction_node(state: SQLAgentState) -> Dict[str, Any]:
    """Generate a corrected SQL from the current candidate and its errors.

    Steps:
    1. Collect validation errors and warnings.
    2. Ask the correction model for a fixed SQL.
    3. Append the corrected candidate to the queue.
    """
    logger.debug("=" * 60)
    logger.debug("Correcting SQL candidate")
    logger.debug("=" * 60)

    question = state["question"]
    candidate_sql = state.get("candidate_sql", "")
    validation_result = state.get("validation_result", {})
    schema_info = state.get("schema_info", {})
    iteration = state.get("iteration", 0)

    errors = list(validation_result.get("errors") or [])
    warnings = validation_result.get("warnings", [])

    logger.debug(f"Correction attempt: {iteration + 1}/{Settings.MAX_RETRY_ATTEMPTS}")
    logger.debug(f"Errors to fix: {errors}")

    if not errors:
        fallback_reason = state.get("error_message") or "execution failed"
        logger.warning(
            "No validation errors; using fallback reason: %s",
            fallback_reason,
        )
        errors = [
            f"{fallback_reason}. Please review the schema and produce a corrected SQL."
        ]

    try:
        schema_description = schema_info.get("schema_description", "")
        corrected_sql, correction_prompt = generate_corrected_sql(
            question=question,
            candidate_sql=candidate_sql,
            errors=errors,
            schema_description=schema_description,
        )

        logger.debug(f"Corrected SQL:\n{corrected_sql}")

        # Append the corrected candidate to the history and queue.
        correction_history = state.get("correction_history", [])
        correction_history.append({
            "iteration": iteration + 1,
            "original_sql": candidate_sql,
            "corrected_sql": corrected_sql,
            "errors": errors,
        })

        generation_candidates = list(state.get("generation_candidates", []))
        new_index = len(generation_candidates)
        generation_candidates.append(
            {
                "index": new_index,
                "sql": corrected_sql,
                "vote_count": 0,
                "is_valid": None,
                "errors": [],
                "source": "correction",
            }
        )
        candidate_queue = list(state.get("candidate_queue", []))
        candidate_queue.append(new_index)

        execution_snapshots = list(state.get("candidate_execution_results", []))
        if len(execution_snapshots) <= new_index:
            execution_snapshots.extend([None] * (new_index + 1 - len(execution_snapshots)))
        execution_snapshots[new_index] = None

        # Reset the current candidate so the dispatcher can pick the new one.
        return {
            "candidate_sql": "",
            "generation_candidates": generation_candidates,
            "candidate_queue": candidate_queue,
            "current_candidate_index": None,
            "current_candidate_meta": {},
            "current_attempt_index": None,
            "iteration": iteration + 1,
            "correction_history": correction_history,
            "messages": state.get("messages", []) + [
                HumanMessage(content=correction_prompt),
                AIMessage(content=corrected_sql),
            ],
            "candidate_execution_results": execution_snapshots,
        }

    except Exception as e:
        logger.error(f"SQL correction failed: {e}")
        return {
            "iteration": iteration + 1,
            "error_message": f"SQL correction failed: {e}",
        }


def format_errors(errors: list) -> str:
    """Format a list of errors into a bulleted string."""

    if not errors:
        return ""

    return "\n".join([f"- {error}" for error in errors])
