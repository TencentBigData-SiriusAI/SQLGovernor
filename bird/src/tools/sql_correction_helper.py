"""Shared helpers for SQL correction prompts."""

from __future__ import annotations

from typing import Iterable

from langchain_core.messages import HumanMessage, SystemMessage
from loguru import logger
from openai import OpenAI

from config import Settings
from config.model_config import get_model_config
from ..utils.llm_client import convert_messages_for_responses, extract_text_from_response
from ..utils.sql_text import extract_sql_from_response
from .sql_projection_rules import apply_projection_rules


SYSTEM_PROMPT = (
    "You are a SQL expert. Given the question, current SQL, schema, and error log, "
    "return a corrected SQL query that answers the question using valid SQLite syntax."
)

PROMPT_TEMPLATE = """Database Engine:
SQLite

Database Schema:
{schema_description}
This schema describes the database’s structure, including tables, columns, primary keys, foreign keys, and any relevant relationships or constraints.

Question:
{question}

Current SQL:
{candidate_sql}

Execution Errors:
{errors}

Instructions:
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- The corrected query should return all of the information asked in the question without any missing or extra information.
- Fix the SQL according to the error messages while preserving the original intent and ensuring valid SQLite syntax.
- Ensure every table and column name exactly matches the schema.

Output Format:
In your answer, please enclose the corrected SQL query in a code block:
```sql
-- Corrected SQL
```

Take a deep breath and think step by step before writing the corrected SQL.
"""


def generate_corrected_sql(
    question: str,
    candidate_sql: str,
    errors: Iterable[str],
    schema_description: str,
) -> tuple[str, str]:
    """Call LLM to correct SQL using shared prompt.

    Returns the corrected SQL and the prompt used.
    """

    target_model_name = (
        Settings.SQL_CORRECTION_MODEL
        or Settings.SQL_GENERATION_MODEL
        or Settings.DEFAULT_MODEL
    )
    model_config = get_model_config(target_model_name)
    try:
        llm = OpenAI(
            base_url=model_config["base_url"],
            api_key=model_config["api_key"],
        )
        model_identifier = model_config["model_name"]
    except Exception as exc:  # pragma: no cover - init failure
        logger.error("Failed to init correction client: %s", exc)
        raise

    prompt = PROMPT_TEMPLATE.format(
        question=question,
        candidate_sql=candidate_sql,
        errors="\n".join(f"- {err}" for err in errors) or "- No errors provided",
        schema_description=schema_description,
    )

    logger.debug("Generating corrected SQL")
    logger.debug(f"Errors: {list(errors)}")
    transport = model_config.get("transport", "chat_completions")
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    try:
        if transport == "responses":
            response = llm.responses.create(
                model=model_identifier,
                input=convert_messages_for_responses(messages),
                temperature=Settings.MODEL_TEMPERATURE,
                max_output_tokens=Settings.MAX_TOKENS,
            )
            content = extract_text_from_response(response)
        else:
            completion = llm.chat.completions.create(
                model=model_identifier,
                messages=messages,
                temperature=Settings.MODEL_TEMPERATURE,
                max_tokens=Settings.MAX_TOKENS,
            )
            content = completion.choices[0].message.content or ""
    except Exception as exc:  # pragma: no cover - api failure
        logger.error("Correction API failed: %s", exc)
        raise

    corrected_sql = extract_sql_from_response(content.strip())
    if Settings.SQL_PROJECTION_RULES_ENABLED and corrected_sql:
        adjusted_sql, adjustments = apply_projection_rules(corrected_sql, question)
        if adjustments:
            corrected_sql = adjusted_sql
            logger.debug(
                "Applied %s projection adjustments", len(adjustments)
            )
    return corrected_sql, prompt
