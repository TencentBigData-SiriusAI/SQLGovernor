"""Shared BIRD sample-loading helpers used by the phase-based pipeline.

This module intentionally keeps gold SQL out of the runtime agent state. Gold SQL remains in
the source BIRD dev file and is read only by the final evaluation script.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.core.state import SQLAgentState


def _remap_legacy_path(path: Path) -> Path:
    """Remap an optional legacy data prefix, otherwise return the expanded path."""

    expanded = path.expanduser()
    legacy_prefix = os.getenv("LEGACY_DATA_PREFIX")
    current_prefix = os.getenv("DATA_DIR")
    if not legacy_prefix or not current_prefix:
        return expanded

    try:
        relative = expanded.relative_to(Path(legacy_prefix).expanduser())
    except ValueError:
        return expanded
    return Path(current_prefix).expanduser() / relative


def _load_json_records(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get("results"), list):
        records = payload["results"]
    else:
        raise ValueError(
            f"Input {path} must be a JSON list or an object containing a 'results' list"
        )
    return [record for record in records if isinstance(record, dict)]


def load_samples(
    input_path: str | Path,
    start_index: int = 0,
    sample_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Load and normalize a slice of BIRD records.

    Raw BIRD `dev.json` and the preprocessed schema-cache format are both accepted. The returned
    samples may still contain their original fields, but `build_initial_state` deliberately does
    not copy gold SQL into the runtime state.
    """

    path = _remap_legacy_path(Path(input_path))
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    if start_index < 0:
        raise ValueError("start_index must be non-negative")
    if sample_count is not None and sample_count < 0:
        sample_count = None

    records = _load_json_records(path)
    stop_index = None if sample_count is None else start_index + sample_count
    selected = records[start_index:stop_index]

    normalized: List[Dict[str, Any]] = []
    for offset, raw_record in enumerate(selected, start=start_index):
        record = dict(raw_record)
        question_id = record.get("question_id", offset)
        db_id = record.get("db_id")
        question = record.get("question")
        if db_id is None or question is None:
            raise ValueError(
                f"Record at index {offset} must contain 'db_id' and 'question'"
            )

        try:
            record["question_id"] = int(question_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid question_id at index {offset}: {question_id!r}"
            ) from exc

        record["db_id"] = str(db_id)
        record["question"] = str(question)
        record["evidence"] = str(
            record.get("evidence") or record.get("external_knowledge") or ""
        )
        if "schema" in record and record["schema"] is not None:
            record["schema"] = str(record["schema"])
        normalized.append(record)

    return normalized


def build_initial_state(
    sample: Dict[str, Any],
    database_path: str,
    model_name: str,
) -> SQLAgentState:
    """Construct the runtime state for schema analysis and SQL generation.

    The `SQL`/`sql` gold fields are intentionally excluded to enforce the offline-only BIRD dev
    protocol. `model_name` is retained as metadata; generation groups are resolved from the model
    profile configuration.
    """

    question_id = sample.get("question_id")
    if question_id is None:
        raise ValueError("Sample is missing question_id")
    db_id = str(sample.get("db_id") or "")
    question = str(sample.get("question") or "")
    if not db_id or not question:
        raise ValueError("Sample must contain non-empty db_id and question")

    return {
        "question_id": int(question_id),
        "db_id": db_id,
        "question": question,
        "evidence": str(sample.get("evidence") or sample.get("external_knowledge") or ""),
        "difficulty": sample.get("difficulty"),
        "database_name": db_id,
        "database_path": database_path,
        "input_schema": str(sample.get("schema") or ""),
        "question_analysis": {},
        "bm25_context": {},
        "schema_info": {},
        "prompt_payload": {},
        "generation_attempts": 0,
        "candidate_sql": "",
        "candidate_available": False,
        "generation_candidates": [],
        "candidate_queue": [],
        "attempted_candidates": [],
        "candidate_execution_results": [],
        "current_candidate_index": None,
        "current_candidate_meta": {},
        "current_attempt_index": None,
        "validation_result": {},
        "execution_result": {},
        "error_message": None,
        "correction_history": [],
        "iteration": 0,
        "messages": [],
        "metadata": {
            "question_id": int(question_id),
            "db_id": db_id,
            "difficulty": sample.get("difficulty"),
            "generation_model": model_name,
        },
        "final_sql": {},
        "scores_payload": [],
    }


__all__ = ["_remap_legacy_path", "build_initial_state", "load_samples"]
