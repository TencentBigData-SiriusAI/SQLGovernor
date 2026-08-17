"""Answer-intent generation for the structured pipeline."""

from __future__ import annotations

import json
import re

from openai import OpenAI

from .llm_retry import chat_completion_with_retry
from .schema_graph import SchemaGraph
from .types import AnswerIntent

SYSTEM_PROMPT = (
    "You are a careful semantic planner for text-to-SQL. "
    "Given a question, evidence, and the full schema context, infer the intended answer shape, "
    "return slots, aggregation intent, ranking intent, and scope intent before any SQL is written. "
    "Do not output SQL."
)

INTENT_TEMPLATE = """Database Engine:
SQLite

Full Database Schema:
{schema}

Question:
{question}

Evidence:
{evidence}

Schema Graph Summary:
{graph_summary}

Task:
Return valid JSON with the following keys:
- answer_shape
- return_slot_count
- return_slots
- aggregate_intent
- ranking_intent
- scope_intent
- formula_intent

Guidelines:
- answer_shape should be one of: scalar, single_row, multi_row, grouped_rows.
- return_slots should describe what the final answer must output, not implementation details.
- value_type should prefer coarse semantic types such as: name, code, description, metric, percentage, date, id, count.
- scope_intent should explicitly mention nested scopes such as filter_then_aggregate, group_then_rank, compare_two_entities, or topk_after_grouping when applicable.
- formula_intent should capture numerator/denominator or max-min style formulas when the question/evidence implies them.
- Use the full schema for disambiguation, but focus on understanding what the question wants returned.
- Do not output markdown.
"""


def build_answer_intent_prompt(
    *,
    schema: str,
    question: str,
    evidence: str,
    graph: SchemaGraph,
) -> str:
    return INTENT_TEMPLATE.format(
        schema=schema,
        question=question,
        evidence=evidence or "(none)",
        graph_summary=graph.summarize(),
    )


def generate_answer_intent(
    *,
    client: OpenAI,
    model_name: str,
    question: str,
    evidence: str,
    schema: str,
    graph: SchemaGraph,
    temperature: float = 0.1,
    max_tokens: int = 2048,
    timeout: int = 120,
) -> AnswerIntent:
    prompt = build_answer_intent_prompt(
        schema=schema,
        question=question,
        evidence=evidence,
        graph=graph,
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
    payload["raw_response"] = raw_content
    intent = AnswerIntent.from_dict(payload)
    return intent or AnswerIntent(raw_response=raw_content)


def _extract_json_payload(text: str) -> dict:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S)
    if fenced:
        text = fenced.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise ValueError("Answer-intent response did not contain a JSON object")
    return json.loads(text[start : end + 1])
