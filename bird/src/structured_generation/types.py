"""Dataclasses for the structured generation MVP."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class AnswerSlot:
    semantic: str
    value_type: str
    role: str = "return"
    candidate_tables: list[str] = field(default_factory=list)
    candidate_columns: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: Any) -> "AnswerSlot":
        if isinstance(payload, str):
            return cls(semantic=payload, value_type="unknown")
        if not isinstance(payload, dict):
            return cls(semantic=str(payload), value_type="unknown")
        semantic = payload.get("semantic")
        if semantic is None:
            semantic = payload.get("column") or payload.get("description") or ""
        return cls(
            semantic=str(semantic or ""),
            value_type=str(payload.get("value_type", "unknown")),
            role=str(payload.get("role", "return")),
            candidate_tables=_normalize_string_list(payload.get("candidate_tables")),
            candidate_columns=_normalize_string_list(payload.get("candidate_columns")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AnswerIntent:
    answer_shape: str = ""
    return_slot_count: int | None = None
    return_slots: list[AnswerSlot] = field(default_factory=list)
    aggregate_intent: str | None = None
    ranking_intent: str | None = None
    scope_intent: list[str] = field(default_factory=list)
    formula_intent: list[str] = field(default_factory=list)
    raw_response: str = ""

    @classmethod
    def from_dict(cls, payload: Any) -> "AnswerIntent | None":
        if not payload:
            return None
        if isinstance(payload, str):
            return cls(answer_shape=payload)
        if not isinstance(payload, dict):
            return cls(answer_shape=str(payload))
        slots_payload = payload.get("return_slots", [])
        if isinstance(slots_payload, dict):
            slots_payload = list(slots_payload.values())
        return cls(
            answer_shape=str(payload.get("answer_shape", "")),
            return_slot_count=_maybe_int(payload.get("return_slot_count")),
            return_slots=[AnswerSlot.from_dict(item) for item in slots_payload],
            aggregate_intent=_maybe_str(payload.get("aggregate_intent")),
            ranking_intent=_maybe_str(payload.get("ranking_intent")),
            scope_intent=_normalize_string_list(payload.get("scope_intent")),
            formula_intent=_normalize_string_list(payload.get("formula_intent")),
            raw_response=str(payload.get("raw_response", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AnchorSpec:
    mention: str
    role: str
    candidate_tables: list[str] = field(default_factory=list)
    candidate_columns: list[str] = field(default_factory=list)
    chosen_table: str | None = None
    chosen_column: str | None = None
    confidence: float | None = None

    @classmethod
    def from_dict(cls, payload: Any) -> "AnchorSpec":
        if isinstance(payload, str):
            return cls(mention=payload, role="unspecified")
        if not isinstance(payload, dict):
            return cls(mention=str(payload), role="unspecified")
        return cls(
            mention=str(payload.get("mention", "")),
            role=str(payload.get("role", "")),
            candidate_tables=_normalize_string_list(payload.get("candidate_tables")),
            candidate_columns=_normalize_string_list(payload.get("candidate_columns")),
            chosen_table=_maybe_str(payload.get("chosen_table")),
            chosen_column=_maybe_str(payload.get("chosen_column")),
            confidence=_maybe_float(payload.get("confidence")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FilterSpec:
    column: str
    operator: str
    value: Any

    @classmethod
    def from_dict(cls, payload: Any) -> "FilterSpec":
        if isinstance(payload, str):
            return cls(column=payload, operator="", value=None)
        if not isinstance(payload, dict):
            return cls(column=str(payload), operator="", value=None)
        return cls(
            column=str(payload.get("column", "")),
            operator=str(payload.get("operator", "")),
            value=payload.get("value"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AggregateSpec:
    function: str
    column: str
    alias: str | None = None

    @classmethod
    def from_dict(cls, payload: Any) -> "AggregateSpec | None":
        if not payload:
            return None
        if isinstance(payload, list):
            if not payload:
                return None
            payload = payload[0]
        if isinstance(payload, str):
            return cls(function="", column=payload, alias=None)
        if not isinstance(payload, dict):
            return cls(function="", column=str(payload), alias=None)
        function = payload.get("function")
        if function is None:
            function = payload.get("operation", "")
        column = payload.get("column")
        if column is None:
            column = payload.get("target", "")
        return cls(
            function=str(function or ""),
            column=str(column or ""),
            alias=_maybe_str(payload.get("alias") or payload.get("description")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OrderingSpec:
    column: str
    direction: str = "ASC"
    limit: int | None = None

    @classmethod
    def from_dict(cls, payload: Any) -> "OrderingSpec | None":
        if not payload:
            return None
        if isinstance(payload, list):
            if not payload:
                return None
            payload = payload[0]
        if isinstance(payload, str):
            return cls(column=payload)
        if not isinstance(payload, dict):
            return cls(column=str(payload))
        return cls(
            column=str(payload.get("column", "")),
            direction=str(payload.get("direction", "ASC")).upper(),
            limit=_maybe_int(payload.get("limit")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OutputSpec:
    columns: list[str] = field(default_factory=list)
    distinct: bool = False

    @classmethod
    def from_dict(cls, payload: Any) -> "OutputSpec":
        if isinstance(payload, list):
            return cls(columns=[str(item) for item in payload], distinct=False)
        if isinstance(payload, str):
            return cls(columns=[payload], distinct=False)
        payload = payload or {}
        return cls(
            columns=_normalize_string_list(payload.get("columns")),
            distinct=bool(payload.get("distinct", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PathPlan:
    path_id: str
    path_kind: str = "structured"
    tables: list[str] = field(default_factory=list)
    join_edges: list[str] = field(default_factory=list)
    bridge_tables: list[str] = field(default_factory=list)
    key_family_choices: dict[str, str] = field(default_factory=dict)
    slot_strategy: str = "default"
    owner_decisions: dict[str, str] = field(default_factory=dict)
    risk_flags: list[str] = field(default_factory=list)
    path_prior: float = 0.0
    rationale: str = ""

    @classmethod
    def from_dict(cls, payload: Any) -> "PathPlan":
        if isinstance(payload, str):
            return cls(path_id="", rationale=payload)
        if not isinstance(payload, dict):
            return cls(path_id="", rationale=str(payload))
        return cls(
            path_id=str(payload.get("path_id", "")),
            path_kind=str(payload.get("path_kind", "structured") or "structured"),
            tables=_normalize_string_list(payload.get("tables")),
            join_edges=_normalize_string_list(payload.get("join_edges")),
            bridge_tables=_normalize_string_list(payload.get("bridge_tables")),
            key_family_choices=_normalize_string_map(payload.get("key_family_choices")),
            slot_strategy=str(payload.get("slot_strategy", "default")),
            owner_decisions=_normalize_string_map(payload.get("owner_decisions")),
            risk_flags=_normalize_string_list(payload.get("risk_flags")),
            path_prior=_normalize_prior(payload.get("path_prior")),
            rationale=str(payload.get("rationale", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StructuredPlan:
    question_id: int | None
    db_id: str
    question: str
    evidence: str
    planner_model: str
    answer_intent: AnswerIntent | None = None
    anchors: list[AnchorSpec] = field(default_factory=list)
    output_spec: OutputSpec = field(default_factory=OutputSpec)
    filter_spec: list[FilterSpec] = field(default_factory=list)
    aggregate_spec: AggregateSpec | None = None
    ordering_spec: OrderingSpec | None = None
    candidate_paths: list[PathPlan] = field(default_factory=list)
    raw_response: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StructuredPlan":
        anchors_payload = payload.get("anchors", [])
        if isinstance(anchors_payload, dict):
            anchors_payload = list(anchors_payload.values())
        filter_payload = payload.get("filter_spec", [])
        if isinstance(filter_payload, dict):
            filter_payload = [filter_payload]
        paths_payload = payload.get("candidate_paths", [])
        if isinstance(paths_payload, dict):
            paths_payload = list(paths_payload.values())
        return cls(
            question_id=_maybe_int(payload.get("question_id")),
            db_id=str(payload.get("db_id", "")),
            question=str(payload.get("question", "")),
            evidence=str(payload.get("evidence", "")),
            planner_model=str(payload.get("planner_model", "")),
            answer_intent=AnswerIntent.from_dict(payload.get("answer_intent")),
            anchors=[AnchorSpec.from_dict(item) for item in anchors_payload],
            output_spec=OutputSpec.from_dict(payload.get("output_spec")),
            filter_spec=[FilterSpec.from_dict(item) for item in filter_payload],
            aggregate_spec=AggregateSpec.from_dict(payload.get("aggregate_spec")),
            ordering_spec=OrderingSpec.from_dict(payload.get("ordering_spec")),
            candidate_paths=[PathPlan.from_dict(item) for item in paths_payload],
            raw_response=str(payload.get("raw_response", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["answer_intent"] = self.answer_intent.to_dict() if self.answer_intent else None
        result["aggregate_spec"] = self.aggregate_spec.to_dict() if self.aggregate_spec else None
        result["ordering_spec"] = self.ordering_spec.to_dict() if self.ordering_spec else None
        return result


@dataclass(slots=True)
class StructuredCandidate:
    question_id: int | None
    db_id: str
    path_id: str
    path_kind: str
    family_id: str
    candidate_id: str
    renderer_model: str
    sql: str
    original_sql: str = ""
    planner_constraints_satisfied: bool = True
    introduced_extra_tables: list[str] = field(default_factory=list)
    introduced_extra_filters: list[str] = field(default_factory=list)
    anti_pattern_flags: list[str] = field(default_factory=list)
    render_score: float = 0.0
    errors: list[str] = field(default_factory=list)
    spec_issues: list[str] = field(default_factory=list)
    semantic_review_issues: list[str] = field(default_factory=list)
    probe_specs: list[dict[str, Any]] = field(default_factory=list)
    probe_findings: list[dict[str, Any]] = field(default_factory=list)
    probe_review_issues: list[str] = field(default_factory=list)
    is_valid: bool = False
    execution: dict[str, Any] = field(default_factory=dict)
    auto_correct_attempts: int = 0
    auto_correct_error: str | None = None
    correction_history: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StructuredCandidate":
        return cls(
            question_id=_maybe_int(payload.get("question_id")),
            db_id=str(payload.get("db_id", "")),
            path_id=str(payload.get("path_id", "")),
            path_kind=str(payload.get("path_kind", "structured") or "structured"),
            family_id=str(payload.get("family_id", "")),
            candidate_id=str(payload.get("candidate_id", "")),
            renderer_model=str(payload.get("renderer_model", "")),
            sql=str(payload.get("sql", "")),
            original_sql=str(payload.get("original_sql", "")),
            planner_constraints_satisfied=bool(payload.get("planner_constraints_satisfied", True)),
            introduced_extra_tables=_normalize_string_list(payload.get("introduced_extra_tables")),
            introduced_extra_filters=_normalize_string_list(payload.get("introduced_extra_filters")),
            anti_pattern_flags=_normalize_string_list(payload.get("anti_pattern_flags")),
            render_score=float(payload.get("render_score", 0.0) or 0.0),
            errors=_normalize_string_list(payload.get("errors")),
            spec_issues=_normalize_string_list(payload.get("spec_issues")),
            semantic_review_issues=_normalize_string_list(payload.get("semantic_review_issues")),
            probe_specs=list(payload.get("probe_specs") or []),
            probe_findings=list(payload.get("probe_findings") or []),
            probe_review_issues=_normalize_string_list(payload.get("probe_review_issues")),
            is_valid=bool(payload.get("is_valid", False)),
            execution=dict(payload.get("execution") or {}),
            auto_correct_attempts=int(payload.get("auto_correct_attempts", 0) or 0),
            auto_correct_error=_maybe_str(payload.get("auto_correct_error")),
            correction_history=list(payload.get("correction_history") or []),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _maybe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_str(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value)
    return value if value else None


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [f"{key}:{item}" for key, item in value.items()]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value]
    return [str(value)]




def _normalize_string_map(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    if isinstance(value, list):
        mapped: dict[str, str] = {}
        for idx, item in enumerate(value):
            if isinstance(item, dict) and "key" in item and "value" in item:
                mapped[str(item["key"])] = str(item["value"])
            else:
                mapped[f"item_{idx}"] = str(item)
        return mapped
    return {"value": str(value)}


def _normalize_prior(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"high", "strong"}:
            return 0.9
        if lowered in {"medium", "moderate"}:
            return 0.6
        if lowered in {"low", "weak"}:
            return 0.3
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
