"""Renderer model interface for structured generation candidates."""

from __future__ import annotations

from typing import Any

from openai import OpenAI

from ..tools import get_few_shot_examples
from .llm_retry import chat_completion_with_retry
from .prompt_rules import build_renderer_rules, render_few_shot_section
from .schema_graph import SchemaGraph
from .spec_check import evaluate_sql_against_plan
from ..tools.sql_validator import extract_table_names, validate_sql_schema, validate_sql_syntax
from ..utils.sql_text import clean_sql_output
from .family_selector import assign_family_id
from .types import PathPlan, StructuredCandidate, StructuredPlan

SYSTEM_PROMPT = (
    "You are a precise SQLite SQL writer. "
    "You are given the full schema and a selected structured plan. "
    "Write SQL that follows the selected path and does not introduce unnecessary tables or filters."
)

RENDER_TEMPLATE = """Database Engine:
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

Output Spec:
{output_payload}

Filter Spec:
{filter_payload}

Aggregate Spec:
{aggregate_payload}

Ordering Spec:
{ordering_payload}

Contextual Examples:
Use these example question-to-SQL pairs to guide SQL phrasing and operator choice. Focus on logic and structure, not exact schema names.
{few_shot_examples}

Rules:
- Prefer the selected path and owner decisions.
- Do not introduce extra tables unless absolutely necessary.
- Respect the selected slot strategy.
- Use the following rendering rules:
{renderer_rules}
- Path handling instructions:
{path_mode_rules}
- Return valid SQLite SQL only.
- Do not emit markdown explanation.

Output:
```sql
SELECT ...
```
"""


def build_renderer_prompt(
    *,
    schema: str,
    plan: StructuredPlan,
    path: PathPlan,
    graph: SchemaGraph | None,
    few_shot_examples: str,
) -> str:
    return RENDER_TEMPLATE.format(
        schema=schema,
        question=plan.question,
        evidence=plan.evidence or "(none)",
        answer_intent=plan.answer_intent.to_dict() if plan.answer_intent else "(none)",
        path_payload=path.to_dict(),
        output_payload=plan.output_spec.to_dict(),
        filter_payload=[item.to_dict() for item in plan.filter_spec],
        aggregate_payload=plan.aggregate_spec.to_dict() if plan.aggregate_spec else None,
        ordering_payload=plan.ordering_spec.to_dict() if plan.ordering_spec else None,
        few_shot_examples=few_shot_examples,
        renderer_rules=build_renderer_rules(plan.question, plan.evidence, plan.db_id, graph),
        path_mode_rules=_build_path_mode_rules(path),
    )


def render_candidates_for_path(
    *,
    client: OpenAI,
    model_name: str,
    plan: StructuredPlan,
    path: PathPlan,
    graph: SchemaGraph | None,
    schema: str,
    schema_info: dict[str, Any],
    database_path: str,
    candidates_per_path: int = 1,
    temperature: float = 0.2,
    freeform_temperature: float | None = None,
    few_shot_top_k: int | None = None,
    few_shot_min_similarity: float | None = None,
    max_tokens: int = 4096,
    timeout: int = 120,
) -> list[StructuredCandidate]:
    effective_top_k = (
        5
        if few_shot_top_k is None
        else few_shot_top_k
    )
    effective_min_similarity = (
        0.0
        if few_shot_min_similarity is None
        else few_shot_min_similarity
    )
    few_shot_examples = []
    if effective_top_k > 0:
        few_shot_examples = get_few_shot_examples(
            question=plan.question,
            evidence=plan.evidence,
            top_k=effective_top_k,
            min_score=effective_min_similarity,
        )
    prompt = build_renderer_prompt(
        schema=schema,
        plan=plan,
        path=path,
        graph=graph,
        few_shot_examples=render_few_shot_section(few_shot_examples),
    )
    effective_temperature = temperature
    if path.path_kind == "freeform":
        effective_temperature = (
            freeform_temperature
            if freeform_temperature is not None
            else max(temperature, 0.7)
        )
    response = chat_completion_with_retry(
        client,
        model=model_name,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=effective_temperature,
        max_tokens=max_tokens,
        n=max(1, candidates_per_path),
        timeout=timeout,
    )

    family_id = assign_family_id(path)
    candidates: list[StructuredCandidate] = []
    for offset, choice in enumerate(response.choices):
        raw_sql = choice.message.content or ""
        sql_text = clean_sql_output(raw_sql)
        syntax_errors = validate_sql_syntax(sql_text) if sql_text else ["EMPTY_SQL"]
        schema_errors = validate_sql_schema(sql_text, schema_info, database_path) if not syntax_errors else []
        spec_issues = evaluate_sql_against_plan(sql_text, plan=plan, path=path) if sql_text else ["EMPTY_SQL"]
        errors = syntax_errors + schema_errors
        extra_tables = sorted(set(extract_table_names(sql_text)) - set(path.tables))
        candidates.append(
            StructuredCandidate(
                question_id=plan.question_id,
                db_id=plan.db_id,
                path_id=path.path_id,
                path_kind=path.path_kind,
                family_id=family_id,
                candidate_id=f"q{plan.question_id}_{path.path_id}_c{offset}",
                renderer_model=model_name,
                sql=sql_text,
                original_sql=sql_text,
                planner_constraints_satisfied=not extra_tables,
                introduced_extra_tables=extra_tables,
                anti_pattern_flags=list(path.risk_flags),
                render_score=max(
                    0.0,
                    path.path_prior
                    - 0.05 * len(errors)
                    - 0.03 * len(spec_issues)
                    - 0.04 * len(extra_tables)
                ),
                errors=errors,
                spec_issues=spec_issues,
                is_valid=not errors,
            )
        )
    return candidates


def _build_path_mode_rules(path: PathPlan) -> str:
    if path.path_kind == "freeform":
        return (
            "- This is a free-form high-variance path. Treat the listed path as a soft hint only.\n"
            "- You may explore an alternative plausible path using the full schema if it better answers the question.\n"
            "- Favor diversity and GT-compatible formulations over strict adherence to the selected path skeleton.\n"
            "- Even in free-form mode, prefer minimal paths and avoid unnecessary detours unless they are needed for correctness."
        )
    return (
        "- This is a structured path. Follow the selected path, owner decisions, key-family choices, and slot strategy as hard constraints whenever possible.\n"
        "- Do not add extra joins if the selected path already reaches the needed fields."
    )
