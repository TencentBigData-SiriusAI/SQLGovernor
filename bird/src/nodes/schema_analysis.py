"""Minimal schema analysis node that loads offline prompts only."""

from typing import Any, Dict

from loguru import logger

from config import Settings
from ..core.state import SQLAgentState
from ..tools import get_offline_prompt_record


def schema_analysis_node(state: SQLAgentState) -> Dict[str, Any]:
    """Load the offline schema prompt for the current sample."""

    logger.debug("=" * 60)
    logger.debug("Loading schema prompt")
    logger.debug("=" * 60)

    question_id = state.get("question_id")
    schema_text = (state.get("input_schema") or "").strip()
    source = "input_sample" if schema_text else None
    record = None

    if not schema_text:
        record = get_offline_prompt_record(question_id)
        if not record:
            logger.error("Missing schema prompt question_id=%s", question_id)
            schema_text = ""
            source = None
        else:
            schema_text = (record.get("schema") or "").strip()
            source = record.get("source")

    schema_info: Dict[str, Any] = {
        "tables": [],
        "foreign_keys": [],
        "schema_description": schema_text,
        "schema_trimmed": False,
        "offline_prompt": bool(record),
        "offline_prompt_path": str(Settings.OFFLINE_SCHEMA_PATH)
        if Settings.OFFLINE_SCHEMA_PATH
        else None,
        "source": source,
    }

    return {
        "schema_info": schema_info,
        "bm25_context": {
            "available": False,
            "values": {},
            "queries": [],
            "summary": "",
            "reason": "offline_only",
        },
        "messages": state.get("messages", []),
    }


__all__ = ["schema_analysis_node"]
