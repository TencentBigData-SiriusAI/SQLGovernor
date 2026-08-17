"""LangGraph state definition for the SQL agent."""

from typing import List, Dict, Any, Optional
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage


class SQLAgentState(TypedDict):
    """Shared state passed between SQL agent nodes.

    Flow:
    question -> question_analysis -> schema_info -> candidate_sql ->
    validation_result -> correction -> execution_result
    """

    # ==================== BIRD inputs ====================
    question_id: int  # question id
    db_id: str  # database id
    question: str  # natural-language question
    evidence: str  # external knowledge / evidence
    SQL: Optional[str]  # ground-truth SQL (used for evaluation only)
    difficulty: Optional[str]  # question difficulty

    # ==================== database ====================
    database_name: str  # database name (db_id)
    database_path: Optional[str]  # path to the database file

    # ==================== schema analysis ====================
    question_analysis: Dict[str, Any]  # analysis result
    # {
    #     "question_type": str,  # question type
    #     "key_entities": List[str],  # key entities
    #     "operations": List[str],  # SQL operations
    # }

    bm25_context: Dict[str, Any]  # schema prompt retrieved via BM25
    schema_info: Dict[str, Any]  # schema information
    # {
    #     "tables": List[Dict],  # tables
    #     "relevant_tables": List[str],  # relevant tables
    #     "foreign_keys": List[Dict],  # foreign keys
    # }

    prompt_payload: Dict[str, Any]  # SQL generation prompt payload
    generation_attempts: int  # number of generation attempts
    candidate_sql: str  # current candidate SQL
    candidate_available: bool  # set by the dispatcher
    generation_candidates: List[Dict[str, Any]]  # generated SQL candidates
    candidate_queue: List[int]  # queue of candidate indices
    attempted_candidates: List[Dict[str, Any]]  # candidates that were attempted
    candidate_execution_results: List[Optional[Dict[str, Any]]]  # per-candidate execution results
    current_candidate_index: Optional[int]  # index into generation_candidates
    current_candidate_meta: Dict[str, Any]  # candidate metadata (index, source, ...)
    current_attempt_index: Optional[int]  # index into attempted_candidates

    validation_result: Dict[str, Any]  # validation result
    # {
    #     "is_valid": bool,  # whether the SQL is valid
    #     "errors": List[str],  # validation errors
    #     "warnings": List[str],  # validation warnings
    # }

    execution_result: Dict[str, Any]  # execution result
    # {
    #     "success": bool,  # whether execution succeeded
    #     "result": Any,  # execution result
    #     "error": Optional[str],  # execution error
    # }

    # ==================== correction loop ====================
    error_message: Optional[str]  # last error message
    correction_history: List[Dict[str, Any]]  # correction history
    iteration: int  # correction iteration counter

    # ==================== messages ====================
    messages: List[BaseMessage]  # LangChain messages

    # ==================== outputs ====================
    metadata: Dict[str, Any]  # sample metadata (question id, db id, ...)
    final_sql: Dict[str, Any]  # final SQL selected by sql_selection
    scores_payload: List[Dict[str, Any]]  # per-candidate scores from sql_selection
