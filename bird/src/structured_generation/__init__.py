"""Structured generation MVP package."""

from .answer_intent import build_answer_intent_prompt, generate_answer_intent
from .correction import apply_correction_attempt, apply_spec_review, needs_correction
from .family_selector import (
    assign_family_id,
    collapse_candidates_by_family,
    select_family_representatives,
    select_winning_candidate,
)
from .planner import build_planner_prompt, generate_structured_plan
from .post_generation_scorer import PostGenerationScore, score_candidate_against_plan
from .probe_review import apply_probe_grounded_review
from .renderer import build_renderer_prompt, render_candidates_for_path
from .schema_graph import SchemaGraph, build_schema_graph
from .semantic_review import apply_semantic_review
from .spec_check import evaluate_sql_against_plan
from .types import (
    AggregateSpec,
    AnswerIntent,
    AnswerSlot,
    AnchorSpec,
    FilterSpec,
    OrderingSpec,
    OutputSpec,
    PathPlan,
    StructuredCandidate,
    StructuredPlan,
)

__all__ = [
    "AggregateSpec",
    "AnswerIntent",
    "AnswerSlot",
    "AnchorSpec",
    "FilterSpec",
    "OrderingSpec",
    "OutputSpec",
    "PathPlan",
    "PostGenerationScore",
    "SchemaGraph",
    "StructuredCandidate",
    "StructuredPlan",
    "apply_correction_attempt",
    "apply_spec_review",
    "assign_family_id",
    "build_answer_intent_prompt",
    "build_planner_prompt",
    "build_renderer_prompt",
    "build_schema_graph",
    "collapse_candidates_by_family",
    "select_family_representatives",
    "generate_structured_plan",
    "apply_probe_grounded_review",
    "score_candidate_against_plan",
    "needs_correction",
    "evaluate_sql_against_plan",
    "apply_semantic_review",
    "render_candidates_for_path",
    "generate_answer_intent",
    "select_winning_candidate",
]
