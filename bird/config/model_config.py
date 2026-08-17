"""Model configuration registry."""

from __future__ import annotations

import os
from typing import Any, Dict


def _env_any(*keys: str, default: str = "none") -> str:
    """Return the first non-empty environment variable in ``keys``."""

    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return default


_sql_base_url = os.getenv("SQL_GENERATION_BASE_URL", "http://0.0.0.0:8080/v1")
_sql_fallback_url = os.getenv("SQL_GENERATION_FALLBACK_BASE_URL", "")
_sql_base_urls = [_sql_base_url]
if _sql_fallback_url and _sql_fallback_url not in _sql_base_urls:
    _sql_base_urls.append(_sql_fallback_url)


MODEL_CONFIGS: Dict[str, Dict[str, Any]] = {
    "qwen3-235b": {
        "base_url": _env_any("MODEL_QWEN3_235B_BASE_URL", "QWEN3_235B_BASE_URL"),
        "api_key": _env_any("MODEL_QWEN3_235B_API_KEY", "QWEN3_235B_API_KEY"),
        "model_name": os.getenv(
            "MODEL_QWEN3_235B_MODEL_NAME",
            os.getenv("QWEN3_235B_MODEL_NAME", "qwen3-235b"),
        ),
        "batch_size": int(os.getenv("MODEL_QWEN3_235B_BATCH_SIZE", "16")),
        "max_batch_size": 16,
        "comment": "External reasoning model.",
    },
    "qwen3-235b": {
        "base_url": _env_any("MODEL_QWEN3_235B_BASE_URL", "QWEN3_235B_BASE_URL"),
        "api_key": _env_any("MODEL_QWEN3_235B_API_KEY", "QWEN3_235B_API_KEY"),
        "model_name": os.getenv(
            "MODEL_QWEN3_235B_MODEL_NAME",
            os.getenv("QWEN3_235B_MODEL_NAME", "qwen3-235b"),
        ),
        "batch_size": int(os.getenv("MODEL_QWEN3_235B_BATCH_SIZE", "16")),
        "max_batch_size": 16,
        "comment": "Backward-compatible alias for the external reasoning model.",
    },
    "sql-generation-model": {
        "base_url": _sql_base_urls[0],
        "base_urls": _sql_base_urls,
        "api_key": os.getenv("SQL_GENERATION_API_KEY", "none"),
        "model_name": os.getenv("SQL_GENERATION_MODEL_NAME", "SQLGOVERNOR-GEN"),
        "comment": "Default SQL generation endpoint.",
    },
    "sql-generation-model-simple": {
        "base_url": os.getenv("SQL_GENERATION_SIMPLE_BASE_URL", _sql_base_urls[0]),
        "api_key": os.getenv(
            "SQL_GENERATION_SIMPLE_API_KEY",
            os.getenv("SQL_GENERATION_API_KEY", "none"),
        ),
        "model_name": os.getenv(
            "SQL_GENERATION_SIMPLE_MODEL_NAME",
            os.getenv("SQL_GENERATION_MODEL_NAME", "SQLGOVERNOR-GEN"),
        ),
        "comment": "SQL generation endpoint for simple questions.",
    },
    "sql-generation-model-moderate": {
        "base_url": os.getenv("SQL_GENERATION_MODERATE_BASE_URL", _sql_base_urls[0]),
        "api_key": os.getenv(
            "SQL_GENERATION_MODERATE_API_KEY",
            os.getenv("SQL_GENERATION_API_KEY", "none"),
        ),
        "model_name": os.getenv(
            "SQL_GENERATION_MODERATE_MODEL_NAME",
            os.getenv("SQL_GENERATION_MODEL_NAME", "SQLGOVERNOR-GEN-V2"),
        ),
        "comment": "SQL generation endpoint for moderate questions.",
    },
    "sql-generation-model-challenging": {
        "base_url": os.getenv("SQL_GENERATION_CHALLENGING_BASE_URL", _sql_base_urls[0]),
        "api_key": os.getenv(
            "SQL_GENERATION_CHALLENGING_API_KEY",
            os.getenv("SQL_GENERATION_API_KEY", "none"),
        ),
        "model_name": os.getenv(
            "SQL_GENERATION_CHALLENGING_MODEL_NAME",
            os.getenv("SQL_GENERATION_MODEL_NAME", "SQLGOVERNOR-GEN-V2"),
        ),
        "comment": "SQL generation endpoint for challenging questions.",
    },
    "gemini-3-pro-preview": {
        "base_url": _env_any(
            "MODEL_GEMINI_3_PRO_BASE_URL",
            "GEMINI_3_PRO_PREVIEW_BASE_URL",
        ),
        "api_key": _env_any(
            "MODEL_GEMINI_3_PRO_API_KEY",
            "GEMINI_3_PRO_PREVIEW_API_KEY",
        ),
        "model_name": os.getenv(
            "MODEL_GEMINI_3_PRO_MODEL_NAME",
            os.getenv("GEMINI_3_PRO_PREVIEW_MODEL_NAME", "gemini-3-pro-preview"),
        ),
        "batch_size": int(os.getenv("MODEL_GEMINI_3_PRO_BATCH_SIZE", "1")),
        "comment": "External secondary reasoning model.",
    },
    "gemini-3-pro-preview-11-2025-thinking": {
        "base_url": _env_any(
            "MODEL_GEMINI_3_PRO_BASE_URL",
            "GEMINI_3_PRO_PREVIEW_BASE_URL",
        ),
        "api_key": _env_any(
            "MODEL_GEMINI_3_PRO_API_KEY",
            "GEMINI_3_PRO_PREVIEW_API_KEY",
        ),
        "model_name": os.getenv(
            "MODEL_GEMINI_3_PRO_MODEL_NAME",
            os.getenv("GEMINI_3_PRO_PREVIEW_MODEL_NAME", "gemini-3-pro-preview"),
        ),
        "batch_size": int(os.getenv("MODEL_GEMINI_3_PRO_BATCH_SIZE", "1")),
        "comment": "Backward-compatible alias for the secondary reasoning model.",
    },
}

MODEL_CONFIGS.setdefault("sqlgovernor-gen", MODEL_CONFIGS["sql-generation-model-simple"])
MODEL_CONFIGS.setdefault("sqlgovernor-gen-v2", MODEL_CONFIGS["sql-generation-model-moderate"])


def _resolve_from_env(key: str) -> Dict[str, Any] | None:
    """Build a model configuration from MODEL_{KEY}_{FIELD} environment variables."""

    prefix = "MODEL_" + key.upper().replace("-", "_") + "_"
    base_url = os.getenv(prefix + "BASE_URL")
    if not base_url:
        return None

    config: Dict[str, Any] = {
        "base_url": base_url,
        "model_name": os.getenv(prefix + "MODEL_NAME", key),
        "api_key": os.getenv(prefix + "API_KEY", "none"),
        "comment": f"Auto-discovered from {prefix}* environment variables.",
    }

    for field in ("BATCH_SIZE", "MAX_BATCH_SIZE"):
        value = os.getenv(prefix + field)
        if value is not None:
            try:
                config[field.lower()] = int(value)
            except ValueError:
                pass

    transport = os.getenv(prefix + "TRANSPORT")
    if transport:
        config["transport"] = transport

    return config


def get_model_config(model_name: str) -> Dict[str, Any]:
    """Return a configured model by key."""

    if model_name in MODEL_CONFIGS:
        return MODEL_CONFIGS[model_name]

    config = _resolve_from_env(model_name)
    if config is not None:
        MODEL_CONFIGS[model_name] = config
        return config

    available_models = ", ".join(MODEL_CONFIGS.keys())
    env_key = "MODEL_" + model_name.upper().replace("-", "_") + "_BASE_URL"
    raise ValueError(
        f"Model '{model_name}' is not configured and {env_key} is not set. "
        f"Available models: {available_models}"
    )


def list_available_models() -> list[str]:
    """List configured model keys."""

    return list(MODEL_CONFIGS.keys())
