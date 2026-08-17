"""Semantic review for generated SQL candidates."""

from __future__ import annotations

import json
import re
from typing import Any

from openai import OpenAI

from ..tools.sql_validator import validate_sql_schema, validate_sql_syntax
from ..utils.sql_text import clean_sql_output
from .llm_retry import chat_completion_with_retry
from .types import PathPlan, StructuredCandidate, StructuredPlan

SYSTEM_PROMPT = (
    "You are a semantic SQL reviewer. "
    "Given the question, evidence, full schema, answer intent, selected path, and current SQL, "
    "judge whether the SQL truly answers the question. "
    "If needed, provide a corrected SQLite SQL query. "
    "Return JSON only."
)

PROMPT_TEMPLATE = """Database Engine:
SQLite

Full Database Schema:
{schema}

Question:
{question}

Evidence:
{evidence}

Answer Intent:
{answer_intent}

Selected Path:
{path_payload}

Current SQL:
{sql}

Task:
Return valid JSON with keys:
- semantic_issues: list[str]
- should_rewrite: bool
- corrected_sql: string

Guidelines:
- Focus on whether the SQL answers the question, not only whether it is executable.
- Pay special attention to output fields, code-vs-description mismatches, aggregation scope, and missing/extra constraints.
- Review mode:
{rewrite_policy}
- If the SQL already answers the question, set should_rewrite=false and corrected_sql=\"\".
- Do not output markdown.
"""


def apply_semantic_review(
    candidate: StructuredCandidate,
    *,
    client: OpenAI,
    model_name: str,
    question: str,
    evidence: str,
    schema_description: str,
    plan: StructuredPlan,
    path: PathPlan,
    schema_info: dict[str, Any],
    database_path: str,
    allow_rewrite: bool = False,
    temperature: float = 0.2,
    max_tokens: int = 3072,
    timeout: int = 120,
) -> StructuredCandidate:
    prompt = PROMPT_TEMPLATE.format(
        schema=schema_description,
        question=question,
        evidence=evidence or "(none)",
        answer_intent=plan.answer_intent.to_dict() if plan.answer_intent else "(none)",
        path_payload=path.to_dict(),
        sql=candidate.sql,
        rewrite_policy=(
            "- Issue-only mode: identify semantic issues, but do not rewrite the SQL. Always set should_rewrite=false and corrected_sql=\"\"."
            if not allow_rewrite
            else "- Rewrite-enabled mode: if the SQL does not answer the question, set should_rewrite=true and provide a corrected SQLite SQL query in corrected_sql."
        ),
    )
    response = chat_completion_with_retry(
        client,
        model=model_name,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    raw_content = response.choices[0].message.content or ""
    payload = _extract_json_payload(raw_content)
    issues = [str(item) for item in payload.get("semantic_issues", [])]
    candidate.semantic_review_issues = issues
    if not allow_rewrite:
        return candidate
    should_rewrite = bool(payload.get("should_rewrite", False))
    corrected_sql = clean_sql_output(str(payload.get("corrected_sql", "")))
    if not should_rewrite or not corrected_sql.strip():
        return candidate

    if not candidate.original_sql:
        candidate.original_sql = candidate.sql
    candidate.sql = corrected_sql.strip()
    candidate.auto_correct_attempts += 1
    syntax_errors = validate_sql_syntax(candidate.sql) if candidate.sql else ["EMPTY_SQL"]
    schema_errors = validate_sql_schema(candidate.sql, schema_info, database_path) if not syntax_errors else []
    candidate.errors = syntax_errors + schema_errors
    candidate.is_valid = not candidate.errors
    candidate.correction_history.append(
        {
            "attempt": candidate.auto_correct_attempts,
            "mode": "semantic_review",
            "semantic_issues": issues,
            "corrected_sql": candidate.sql,
        }
    )
    return candidate


def _extract_json_payload(text: str) -> dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S)
    if fenced:
        text = fenced.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise ValueError("Semantic review response did not contain a JSON object")
    return json.loads(text[start : end + 1])
