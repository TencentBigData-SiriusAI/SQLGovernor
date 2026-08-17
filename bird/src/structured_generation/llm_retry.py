"""Retry helpers for transient LLM service errors."""

from __future__ import annotations

import time
from typing import Any


def chat_completion_with_retry(
    client: Any,
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float,
    max_tokens: int,
    timeout: int,
    n: int | None = None,
    max_attempts: int = 3,
    backoff_seconds: float = 1.5,
):
    kwargs = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "timeout": timeout,
    }
    if n is not None:
        kwargs["n"] = n

    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return client.chat.completions.create(**kwargs)
        except Exception as exc:  # pragma: no cover - network/provider instability
            last_exc = exc
            if attempt >= max_attempts or not _is_retryable(exc):
                raise
            time.sleep(backoff_seconds * attempt)
    raise last_exc or RuntimeError("chat completion retry exhausted")


def _is_retryable(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    retry_tokens = (
        "502",
        "503",
        "504",
        "connection error",
        "api connection",
        "timeout",
        "timed out",
        "temporarily unavailable",
        "internal server error",
        "bad gateway",
        "rate limit",
    )
    return any(token in text for token in retry_tokens)
