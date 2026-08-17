"""Offline prompt cache loader for prebuilt BIRD schemas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from config import Settings

_CACHE: Dict[int, Dict[str, Any]] = {}
_LOADED = False


def _load_cache() -> None:
    """Load the offline prompt JSON into memory once."""
    global _LOADED
    if _LOADED:
        return

    prompt_path: Optional[Path] = Settings.OFFLINE_SCHEMA_PATH
    if not prompt_path:
        logger.debug("OFFLINE_SCHEMA_PATH not set; skipping prompt cache")
        _LOADED = True
        return
    if not prompt_path.exists():
        logger.warning(f"Prompt cache missing: {prompt_path}")
        _LOADED = True
        return

    try:
        with prompt_path.open("r", encoding="utf-8") as handle:
            records = json.load(handle)
    except Exception as exc:  # pragma: no cover - load failure
        logger.warning(f"Failed to load prompt cache: {exc}")
        _LOADED = True
        return

    loaded = 0
    for record in records:
        question_id = record.get("question_id")
        if not isinstance(question_id, int):
            continue
        _CACHE[question_id] = record
        loaded += 1

    logger.info(f"Loaded {loaded} prompt records")
    _LOADED = True


def get_offline_prompt_record(question_id: Optional[int]) -> Optional[Dict[str, Any]]:
    """Return the offline prompt record for the given question id."""
    if question_id is None:
        return None
    _load_cache()
    return _CACHE.get(question_id)


__all__ = ["get_offline_prompt_record"]
