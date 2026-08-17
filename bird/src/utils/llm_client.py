"""LLM client helpers supporting ChatCompletion and Responses APIs."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from langchain_core.messages import BaseMessage
from langchain_openai import ChatOpenAI
from loguru import logger
from openai import OpenAI

from config import Settings, get_model_config


def _stream_is_tty(stream: Any) -> bool:
    isatty = getattr(stream, "isatty", None)
    if not callable(isatty):
        return False
    try:
        return bool(isatty())
    except Exception:
        return False


def get_llm_client(
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> Any:
    """Create an LLM client from a model name and optional overrides.

    Args:
        model_name: Model key; defaults to Settings.DEFAULT_MODEL.
        temperature: Sampling temperature; defaults to Settings.MODEL_TEMPERATURE.
        max_tokens: Max output tokens; defaults to Settings.MAX_TOKENS.
        timeout: Request timeout; defaults to Settings.REQUEST_TIMEOUT.

    Returns:
        A ChatOpenAI (or ResponsesClientAdapter) instance.
    """
    # Resolve defaults from settings.
    model_name = model_name or Settings.DEFAULT_MODEL
    temperature = temperature if temperature is not None else Settings.MODEL_TEMPERATURE
    max_tokens = max_tokens or Settings.MAX_TOKENS
    timeout = timeout or Settings.REQUEST_TIMEOUT
    
    config = get_model_config(model_name)

    base_urls = config.get("base_urls") or []
    if not base_urls and config.get("base_url"):
        base_urls = [config["base_url"]]
    if not base_urls:
        raise ValueError(f"Model '{model_name}' has no base_url")

    last_error: Exception | None = None

    for idx, candidate_base_url in enumerate(base_urls):
        try:
            logger.info(
                f"Initializing LLM client: {model_name} (base_url={candidate_base_url})"
            )
            logger.debug(
                f"LLM params: temperature={temperature}, max_tokens={max_tokens}, timeout={timeout}"
            )

            if config.get("transport") == "responses":
                client = ResponsesClientAdapter(
                    base_url=candidate_base_url,
                    api_key=config["api_key"],
                    model=config["model_name"],
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout=timeout,
                )
            else:
                client = ChatOpenAI(
                    base_url=candidate_base_url,
                    api_key=config["api_key"],
                    model=config["model_name"],
                    temperature=temperature,
                    max_tokens=max_tokens,
                    max_retries=2,
                    request_timeout=timeout,
                )

            if idx > 0:
                logger.info(f"Falling back to base_url: {candidate_base_url}")

            return client

        except Exception as exc:  # pragma: no cover - init failure
            last_error = exc
            logger.warning(
                f"LLM client init failed (base_url={candidate_base_url}): {exc}"
            )

    # All base_urls failed.
    assert last_error is not None
    logger.error(f"All LLM clients failed: {last_error}")
    raise last_error


def setup_logging(log_level: str = None):
    """Configure loguru console and file handlers.

    Args:
        log_level: One of DEBUG, INFO, WARNING, ERROR.
    """
    log_level = log_level or Settings.LOG_LEVEL

    # Remove the default handler.
    logger.remove()

    console_sink = sys.stdout

    # Colorize only when attached to a TTY.
    logger.add(
        console_sink,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=_stream_is_tty(console_sink),
        enqueue=True,
    )
    
    # Add a file handler.
    log_file = Settings.LOG_FILE
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=log_level,
        rotation="10 MB",  # rotate at 10MB
        retention="7 days",  # retain for 7 days
        compression="zip",  # compress rotated files
        enqueue=True,
    )
    
    logger.info(f"Logging initialized, level: {log_level}")
    logger.info(f"Log file: {log_file}")


# Initialize logging on import.
setup_logging()


@dataclass
class _SimpleResponse:
    content: str


def _response_message_to_dict(message: BaseMessage) -> dict[str, str]:
    role_map = {
        "human": "user",
        "user": "user",
        "ai": "assistant",
        "assistant": "assistant",
        "system": "system",
    }
    role = role_map.get(getattr(message, "type", "user"), "user")
    content = getattr(message, "content", "")
    return {"role": role, "content": content}


def convert_messages_for_responses(messages: Any) -> list[dict[str, str]]:
    if isinstance(messages, str):
        return [{"role": "user", "content": messages}]

    if isinstance(messages, BaseMessage):
        return [_response_message_to_dict(messages)]

    if isinstance(messages, Iterable):
        converted: list[dict[str, str]] = []
        for msg in messages:
            if isinstance(msg, BaseMessage):
                converted.append(_response_message_to_dict(msg))
            elif isinstance(msg, dict):
                role = msg.get("role", "user")
                content = msg.get("content", "")
                converted.append({"role": role, "content": str(content)})
            else:
                converted.append({"role": "user", "content": str(msg)})
        return converted or [{"role": "user", "content": ""}]

    return [{"role": "user", "content": str(messages)}]


def extract_text_from_response(response: Any) -> str:
    # Extract text from Responses API structured output.
    output = getattr(response, "output", None)
    if output:
        texts: list[str] = []
        for block in output:
            for content in getattr(block, "content", []) or []:
                text = getattr(content, "text", None)
                if text:
                    value = getattr(text, "value", None)
                    texts.append(value or str(text))
        if texts:
            return "\n".join(t.strip() for t in texts if t).strip()

    # Fall back to model_dump/json parsing.
    try:
        data = response.model_dump()
    except AttributeError:
        data = getattr(response, "__dict__", response)

    if isinstance(data, dict):
        outputs = data.get("output") or data.get("choices")
        if outputs:
            pieces: list[str] = []
            for item in outputs:
                content = item.get("content") or item.get("message")
                if isinstance(content, list):
                    for c in content:
                        text = c.get("text") if isinstance(c, dict) else c
                        if isinstance(text, dict):
                            text = text.get("value")
                        if text:
                            pieces.append(str(text))
                elif isinstance(content, dict):
                    text = content.get("text") or content.get("content")
                    if isinstance(text, dict):
                        text = text.get("value")
                    if text:
                        pieces.append(str(text))
                elif content:
                    pieces.append(str(content))
            if pieces:
                return "\n".join(pieces).strip()

    return str(response)


class ResponsesClientAdapter:
    """Adapter exposing the OpenAI Responses API with a LangChain-style invoke."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        temperature: float,
        max_tokens: int,
        timeout: int,
    ) -> None:
        self._client = OpenAI(base_url=base_url, api_key=api_key)
        self._model = model
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._timeout = timeout

    def invoke(self, messages: Any) -> _SimpleResponse:
        payload = convert_messages_for_responses(messages)
        response = self._client.responses.create(
            model=self._model,
            input=payload,
            temperature=self._temperature,
            max_output_tokens=self._max_tokens,
            timeout=self._timeout,
        )
        text = extract_text_from_response(response)
        return _SimpleResponse(text)
