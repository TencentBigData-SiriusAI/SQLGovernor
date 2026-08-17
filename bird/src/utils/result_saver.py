"""Helpers for persisting SQL generation snapshots."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from loguru import logger

from config import Settings


def save_generation_snapshot(record: Dict[str, Any]) -> None:
    """Persist a single question's generation record to disk."""

    if not Settings.SQL_GENERATION_SAVE_RESULTS:
        return

    output_dir = Path(Settings.SQL_GENERATION_SAVE_DIR)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # pragma: no cover - IO errors
        logger.error(f"Failed to create output dir {output_dir}: {exc}")
        return

    question_id = record.get("question_id", "unknown")
    db_id = record.get("db_id", "unknown") or "unknown"
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"q{question_id}_{db_id}_{timestamp}.json"
    filepath = output_dir / filename

    try:
        with filepath.open("w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)
        logger.debug(f"Saved SQL snapshot question_id={question_id} to {filepath}")
    except Exception as exc:  # pragma: no cover - IO errors
        logger.error(f"Failed to save SQL snapshot {filepath}: {exc}")

