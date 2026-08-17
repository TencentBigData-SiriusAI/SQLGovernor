"""Planner model interface for the structured generation MVP."""

from __future__ import annotations

import json
import re
from typing import Any

from openai import OpenAI

from ..tools import get_few_shot_examples
from .llm_retry import chat_completion_with_retry
from .prompt_rules import build_planner_rules, render_few_shot_section
from .schema_graph import SchemaGraph
from .types import AnswerIntent, PathPlan, StructuredPlan

SYSTEM_PROMPT = (
    "You are a careful SQL planning assistant. "
    "Given a full database schema, question, evidence, and a schema graph summary, "
    "produce a compact JSON plan before SQL is written. "
    "Do not output SQL."
)

PLAN_TEMPLATE = """Database Engine:
SQLite

Full Database Schema:
{schema}

Question:
{question}

Evidence:
{evidence}

Schema Graph Summary:
{graph_summary}

Answer Intent:
{answer_intent}

Contextual Examples:
To help you, here are some example question-to-SQL pairs. Use them to understand query structure and reasoning patterns, but do not copy table or column names blindly.
{few_shot_examples}

Task:
Produce a JSON plan with the following top-level keys:
- question_id
- db_id
- question
- evidence
- planner_model
- anchors
- output_spec
- filter_spec
- aggregate_spec
- ordering_spec
- candidate_paths

Rules:
- candidate_paths must contain between 1 and {max_paths} paths.
- Each path must specify tables, join_edges, bridge_tables, key_family_choices, slot_strategy, owner_decisions, risk_flags, path_prior, rationale.
- Prefer concise, minimal, plausible paths.
- Include at least one shortest plausible path when possible.
- Candidate paths should reflect distinct structural hypotheses instead of trivial rewrites of the same skeleton.
- Anchors should map question concepts to concrete owner tables/columns whenever possible.
- Use the following planning rules:
{planner_rules}
- Do not emit markdown.
- Return valid JSON only.
"""


def build_planner_prompt(
    *,
    schema: str,
    question: str,
    evidence: str,
    db_id: str,
    graph: SchemaGraph,
    max_paths: int,
    answer_intent: AnswerIntent | None,
    few_shot_examples: str,
) -> str:
    return PLAN_TEMPLATE.format(
        schema=schema,
        question=question,
        evidence=evidence or "(none)",
        graph_summary=graph.summarize(),
        answer_intent=answer_intent.to_dict() if answer_intent else "(none)",
        few_shot_examples=few_shot_examples,
        max_paths=max_paths,
        planner_rules=build_planner_rules(question, evidence, db_id, graph),
    )


def generate_structured_plan(
    *,
    client: OpenAI,
    model_name: str,
    question_id: int | None,
    db_id: str,
    question: str,
    evidence: str,
    schema: str,
    graph: SchemaGraph,
    answer_intent: AnswerIntent | None = None,
    max_paths: int = 3,
    temperature: float = 0.1,
    few_shot_top_k: int | None = None,
    few_shot_min_similarity: float | None = None,
    max_tokens: int = 4096,
    timeout: int = 120,
) -> StructuredPlan:
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
            question=question,
            evidence=evidence,
            top_k=effective_top_k,
            min_score=effective_min_similarity,
        )
    prompt = build_planner_prompt(
        schema=schema,
        question=question,
        evidence=evidence,
        db_id=db_id,
        graph=graph,
        max_paths=max_paths,
        answer_intent=answer_intent,
        few_shot_examples=render_few_shot_section(few_shot_examples),
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
    payload["question_id"] = question_id
    payload["db_id"] = db_id
    payload["question"] = question
    payload["evidence"] = evidence
    payload["planner_model"] = model_name
    if answer_intent is not None:
        payload["answer_intent"] = answer_intent.to_dict()
    payload["raw_response"] = raw_content
    plan = StructuredPlan.from_dict(payload)
    structured_paths = []
    for idx, path in enumerate(plan.candidate_paths, start=1):
        if not path.path_id:
            path.path_id = f"p{idx}"
        path.path_kind = path.path_kind or "structured"
        structured_paths.append(path)

    if max_paths > 1:
        structured_paths = sorted(structured_paths, key=lambda item: item.path_prior, reverse=True)
        keep_n = max(0, max_paths - 1)
        structured_paths = structured_paths[:keep_n]
        structured_paths.append(_build_freeform_path(plan, structured_paths))
    else:
        structured_paths = structured_paths[:1] if structured_paths else [_build_freeform_path(plan, [])]

    for idx, path in enumerate(structured_paths, start=1):
        if not path.path_id:
            path.path_id = f"p{idx}"
    plan.candidate_paths = structured_paths
    return plan


def _extract_json_payload(text: str) -> dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S)
    if fenced:
        text = fenced.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise ValueError("Planner response did not contain a JSON object")
    return json.loads(text[start : end + 1])


def _build_freeform_path(plan: StructuredPlan, paths: list[PathPlan]) -> PathPlan:
    anchor_tables = sorted(
        {
            anchor.chosen_table
            for anchor in plan.anchors
            if anchor.chosen_table
        }
    )
    inherited_tables = paths[0].tables if paths else []
    inherited_edges = paths[0].join_edges if paths else []
    return PathPlan(
        path_id="p_freeform",
        path_kind="freeform",
        tables=anchor_tables or list(inherited_tables),
        join_edges=list(inherited_edges),
        bridge_tables=[],
        key_family_choices={},
        slot_strategy="freeform",
        owner_decisions={},
        risk_flags=["high_variance_freeform"],
        path_prior=0.2,
        rationale="Exploratory high-variance path that may deviate from the structured path to improve coverage.",
    )
