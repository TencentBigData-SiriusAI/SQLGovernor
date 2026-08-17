"""Correction helpers for structured generation candidates."""

from __future__ import annotations

from typing import Any

from openai import OpenAI

from ..tools import generate_corrected_sql
from ..tools.sql_validator import validate_sql_schema, validate_sql_syntax
from ..utils.sql_text import clean_sql_output
from .llm_retry import chat_completion_with_retry
from .spec_check import evaluate_sql_against_plan
from .types import PathPlan, StructuredCandidate, StructuredPlan


def needs_correction(candidate: StructuredCandidate) -> bool:
    if (
        candidate.execution.get("success")
        and not candidate.errors
        and not candidate.spec_issues
        and not candidate.probe_review_issues
    ):
        return False
    return True


def apply_correction_attempt(
    candidate: StructuredCandidate,
    *,
    question: str,
    schema_description: str,
    schema_info: dict[str, Any],
    database_path: str,
) -> StructuredCandidate:
    error_messages = list(candidate.errors) + list(candidate.spec_issues) + list(candidate.probe_review_issues)
    execution_error = candidate.execution.get("error")
    if execution_error:
        error_messages.append(str(execution_error))
    if not error_messages:
        error_messages.append("Unknown failure, please correct the SQL.")

    corrected_sql, _ = generate_corrected_sql(
        question=question,
        candidate_sql=candidate.sql,
        errors=error_messages,
        schema_description=schema_description,
    )
    candidate.auto_correct_attempts += 1
    if not corrected_sql.strip():
        candidate.auto_correct_error = "empty_corrected_sql"
        return candidate

    updated_sql = corrected_sql.strip()
    if not candidate.original_sql:
        candidate.original_sql = candidate.sql
    candidate.sql = updated_sql
    syntax_errors = validate_sql_syntax(updated_sql) if updated_sql else ["EMPTY_SQL"]
    schema_errors = validate_sql_schema(updated_sql, schema_info, database_path) if not syntax_errors else []
    candidate.errors = syntax_errors + schema_errors
    candidate.is_valid = not candidate.errors
    candidate.auto_correct_error = None
    candidate.correction_history.append(
        {
            "attempt": candidate.auto_correct_attempts,
            "error_messages": error_messages,
            "corrected_sql": updated_sql,
            "mode": "generic_correction",
        }
    )
    return candidate


def apply_spec_review(
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
    temperature: float = 0.2,
    max_tokens: int = 4096,
    timeout: int = 120,
) -> StructuredCandidate:
    spec_issues = evaluate_sql_against_plan(candidate.sql, plan=plan, path=path)
    candidate.spec_issues = list(spec_issues)
    if not spec_issues:
        return candidate

    prompt = _build_spec_review_prompt(
        question=question,
        evidence=evidence,
        schema_description=schema_description,
        candidate_sql=candidate.sql,
        plan=plan,
        path=path,
        spec_issues=spec_issues,
    )
    response = chat_completion_with_retry(
        client,
        model=model_name,
        messages=[
            {"role": "system", "content": _SPEC_REVIEW_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    reviewed_sql = clean_sql_output(response.choices[0].message.content or "")
    if not reviewed_sql.strip():
        candidate.auto_correct_error = "empty_spec_review_sql"
        return candidate

    if not candidate.original_sql:
        candidate.original_sql = candidate.sql
    candidate.sql = reviewed_sql.strip()
    candidate.auto_correct_attempts += 1
    syntax_errors = validate_sql_syntax(candidate.sql) if candidate.sql else ["EMPTY_SQL"]
    schema_errors = validate_sql_schema(candidate.sql, schema_info, database_path) if not syntax_errors else []
    candidate.errors = syntax_errors + schema_errors
    candidate.is_valid = not candidate.errors
    candidate.spec_issues = evaluate_sql_against_plan(candidate.sql, plan=plan, path=path)
    candidate.auto_correct_error = None
    candidate.correction_history.append(
        {
            "attempt": candidate.auto_correct_attempts,
            "error_messages": list(spec_issues),
            "corrected_sql": candidate.sql,
            "mode": "spec_review",
        }
    )
    return candidate


_SPEC_REVIEW_SYSTEM_PROMPT = (
    "You are a precise SQL reviewer. "
    "Given a question, evidence, schema, selected path, query spec, current SQL, and detected spec issues, "
    "return a corrected SQLite SQL query that preserves the intended answer and better matches the query spec."
)


def _build_spec_review_prompt(
    *,
    question: str,
    evidence: str,
    schema_description: str,
    candidate_sql: str,
    plan: StructuredPlan,
    path: PathPlan,
    spec_issues: list[str],
) -> str:
    return f"""Database Engine:
SQLite

Database Schema:
{schema_description}

Question:
{question}

Evidence:
{evidence or "(none)"}

Selected Path:
{path.to_dict()}

Query Spec:
{{
  "output_spec": {plan.output_spec.to_dict()},
  "filter_spec": {[item.to_dict() for item in plan.filter_spec]},
  "aggregate_spec": {plan.aggregate_spec.to_dict() if plan.aggregate_spec else None},
  "ordering_spec": {plan.ordering_spec.to_dict() if plan.ordering_spec else None}
}}

Current SQL:
{candidate_sql}

Detected Spec Issues:
{chr(10).join("- " + issue for issue in spec_issues)}

Instructions:
- Fix the SQL so it matches the query spec more closely.
- Do not add extra columns, filters, or tables unless they are strictly required.
- Keep the SQL valid for SQLite.
- Output SQL only, inside a ```sql``` block.
"""
