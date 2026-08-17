"""LangGraph definition for the SQL agent pipeline."""

from typing import Literal
from langgraph.graph import StateGraph, END
from loguru import logger

from .state import SQLAgentState
from ..nodes import (
    schema_analysis_node,
    sql_generation_node,
    sql_candidate_dispatch_node,
    sql_validation_node,
    sql_correction_node,
    sql_execution_node,
    sql_selection_node,
)
from config import Settings


def should_dispatch_next(state: SQLAgentState) -> Literal["validation", "correction", "generation"]:
    """Determine whether the dispatcher has a candidate ready."""

    if state.get("candidate_available"):
        return "validation"

    queue = state.get("candidate_queue") or []
    candidates = state.get("generation_candidates") or []
    generation_attempts = state.get("generation_attempts", 0)
    if not queue and not candidates:
        if generation_attempts < Settings.SQL_GENERATION_EMPTY_RETRY_LIMIT:
            logger.warning(f"No candidates; retrying SQL generation (attempt {generation_attempts + 1})")
            return "generation"

    logger.warning("No candidates available; moving to correction")
    return "correction"


def post_validation_route(state: SQLAgentState) -> Literal["execution", "dispatch", "correction"]:
    """Decide the next step after validation completes."""

    validation = state.get("validation_result", {})
    if validation.get("is_valid"):
        logger.info("SQL is valid; moving to execution")
        return "execution"

    queue = state.get("candidate_queue") or []
    if queue:
        logger.warning("SQL invalid but more candidates remain; dispatching next")
        return "dispatch"

    logger.warning("SQL invalid and no candidates remain; moving to correction")
    return "correction"


def post_execution_route(state: SQLAgentState) -> Literal["selection", "dispatch", "correction"]:
    """Decide the next step after executing the current candidate."""

    execution = state.get("execution_result", {})
    queue = state.get("candidate_queue") or []
    if queue:
        if execution.get("success"):
            logger.info("Execution succeeded; dispatching next candidate")
        else:
            logger.warning("Execution failed; dispatching next candidate")
        return "dispatch"

    if execution.get("success"):
        logger.info("Execution succeeded and no candidates remain; selecting final SQL")
        return "selection"

    iteration = state.get("iteration", 0)
    if iteration < Settings.MAX_RETRY_ATTEMPTS:
        logger.warning("Execution failed; moving to correction")
        return "correction"

    logger.error("Max retry attempts reached; selecting best available SQL")
    return "selection"


def create_sql_agent_graph() -> StateGraph:
    """Build the SQL agent state graph.

    Nodes:
    1. Schema analysis (schema_analysis)
    2. SQL generation (sql_generation)
    3. SQL validation (sql_validation)
    4. Routing:
       - valid -> SQL execution (sql_execution)
       - invalid -> SQL correction (sql_correction) -> re-dispatch
       - no candidates -> generation or correction
    5. SQL selection (sql_selection)
    6. End (END)

    Returns:
        StateGraph
    """
    # Build the state graph.
    workflow = StateGraph(SQLAgentState)

    # ==================== nodes ====================
    logger.info("Building SQL agent graph...")

    workflow.add_node("schema_analysis", schema_analysis_node)
    workflow.add_node("sql_generation", sql_generation_node)
    workflow.add_node("sql_candidate_dispatch", sql_candidate_dispatch_node)
    workflow.add_node("sql_validation", sql_validation_node)
    workflow.add_node("sql_correction", sql_correction_node)
    workflow.add_node("sql_execution", sql_execution_node)
    workflow.add_node("sql_selection", sql_selection_node)

    # ==================== entry point ====================
    workflow.set_entry_point("schema_analysis")

    # ==================== edges ====================
    workflow.add_edge("schema_analysis", "sql_generation")
    workflow.add_edge("sql_generation", "sql_candidate_dispatch")

    # Conditional routing after dispatch.
    workflow.add_conditional_edges(
        "sql_candidate_dispatch",
        should_dispatch_next,
        {
            "validation": "sql_validation",
            "correction": "sql_correction",
            "generation": "sql_generation",
        },
    )

    # Conditional routing after validation.
    workflow.add_conditional_edges(
        "sql_validation",
        post_validation_route,
        {
            "execution": "sql_execution",
            "dispatch": "sql_candidate_dispatch",
            "correction": "sql_correction",
        },
    )

    # Correction loops back to the dispatcher.
    workflow.add_edge("sql_correction", "sql_candidate_dispatch")

    # Conditional routing after execution.
    workflow.add_conditional_edges(
        "sql_execution",
        post_execution_route,
        {
            "selection": "sql_selection",
            "dispatch": "sql_candidate_dispatch",
            "correction": "sql_correction",
        },
    )

    workflow.add_edge("sql_selection", END)

    logger.info("SQL agent graph built")

    return workflow


def compile_sql_agent_graph():
    """Compile the SQL agent graph for execution.

    Returns:
        Compiled graph.
    """
    workflow = create_sql_agent_graph()
    app = workflow.compile()

    logger.info("SQL agent graph compiled")

    return app


def invoke_sql_agent(app, initial_state, extra_config=None):
    """Invoke the compiled graph with the standard recursion limit config."""

    config = dict(extra_config or {})
    config.setdefault("recursion_limit", Settings.GRAPH_RECURSION_LIMIT)
    return app.invoke(initial_state, config=config)
