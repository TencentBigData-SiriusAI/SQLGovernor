from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from typing import Any

from sqlglot import exp, parse_one

from error_bank.anchor_swap_hints import search_anchor_swap_hints
from error_bank.date_anchor_hints import search_date_anchor_hints
from error_bank.prober import Diagnosis
from error_bank.value_search import search_literal_in_query_tables


_FIELD_KEYWORDS = [
    "id",
    "name",
    "title",
    "language",
    "translation",
    "code",
    "date",
    "year",
    "month",
    "day",
    "amount",
    "price",
    "cost",
    "expense",
    "consumption",
    "count",
    "number",
    "sum",
    "total",
    "average",
    "avg",
    "owner",
    "editor",
    "patient",
    "user",
    "city",
    "county",
    "state",
    "country",
]

_COMPARISON_PATTERNS = [
    r"\bmore than\b[^,.?;]*",
    r"\bless than\b[^,.?;]*",
    r"\bat least\b[^,.?;]*",
    r"\bat most\b[^,.?;]*",
    r"\bbefore\b[^,.?;]*",
    r"\bafter\b[^,.?;]*",
    r"\bbetween\b[^,.?;]*",
    r"\bgreater than\b[^,.?;]*",
    r"\blower than\b[^,.?;]*",
    r"\bunder\b[^,.?;]*",
    r"\bover\b[^,.?;]*",
]


@dataclass(slots=True)
class QueryProjection:
    expression: str
    alias: str = ""
    source_columns: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QueryJoin:
    join_type: str
    table: str
    alias: str = ""
    on: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QueryPredicate:
    clause: str
    expression: str
    columns: list[str] = field(default_factory=list)
    literals: list[str] = field(default_factory=list)
    operator: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class LiteralBinding:
    clause: str
    column: str
    operator: str
    literal: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TaskSpec:
    answer_shape: str = "unknown"
    target_entity_hint: str = ""
    requested_fields: list[str] = field(default_factory=list)
    protected_literals: list[str] = field(default_factory=list)
    mandatory_constraints: list[str] = field(default_factory=list)
    aggregation_intent: str = "none"
    boolean_intent: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QuerySpec:
    sql_text: str
    projections: list[QueryProjection] = field(default_factory=list)
    base_tables: list[str] = field(default_factory=list)
    joins: list[QueryJoin] = field(default_factory=list)
    where_predicates: list[QueryPredicate] = field(default_factory=list)
    having_predicates: list[QueryPredicate] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    limit: int | None = None
    set_operation: str = ""
    subqueries: list[str] = field(default_factory=list)
    literal_bindings: list[LiteralBinding] = field(default_factory=list)
    parse_error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FailureStageTrace:
    stage_idx: int
    stage_type: str
    condition_text: str
    row_count: int
    drop_from_prev: int | None = None
    is_killer: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FailureSpec:
    failure_stage: str = "unknown"
    killer_unit: str = ""
    root_cause_type: str = "UNKNOWN"
    root_cause_detail: str = ""
    schema_anchors: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    stage_trace: list[FailureStageTrace] = field(default_factory=list)
    counterfactual_hint: str = ""
    value_probe_facts: list[dict[str, Any]] = field(default_factory=list)
    join_probe_facts: list[dict[str, Any]] = field(default_factory=list)
    in_query_value_hints: list[dict[str, Any]] = field(default_factory=list)
    date_anchor_hints: list[dict[str, Any]] = field(default_factory=list)
    anchor_swap_hints: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "stage_trace": [item.to_dict() for item in self.stage_trace],
        }


@dataclass(slots=True)
class PatchPlan:
    patch_goal: str
    edit_targets: list[str] = field(default_factory=list)
    allowed_repairs: list[str] = field(default_factory=list)
    forbidden_repairs: list[str] = field(default_factory=list)
    protected_literals: list[str] = field(default_factory=list)
    answer_shape_guard: str = "preserve"
    rationale: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AlignmentError:
    slot: str
    misalignment_type: str
    observed_sql_unit: str = ""
    evidence_summary: str = ""
    suggested_actions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AlignmentSpec:
    semantic_slots: dict[str, Any] = field(default_factory=dict)
    sql_slots: dict[str, Any] = field(default_factory=dict)
    alignment_errors: list[AlignmentError] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "semantic_slots": self.semantic_slots,
            "sql_slots": self.sql_slots,
            "alignment_errors": [item.to_dict() for item in self.alignment_errors],
        }


@dataclass(slots=True)
class StructuredDiagnosisBundle:
    question_id: int
    db_id: str
    question: str
    evidence: str
    original_sql: str
    task_spec: TaskSpec
    query_spec: QuerySpec
    failure_spec: FailureSpec
    patch_plan: PatchPlan
    alignment_spec: AlignmentSpec

    def to_dict(self) -> dict[str, Any]:
        return {
            "question_id": self.question_id,
            "db_id": self.db_id,
            "question": self.question,
            "evidence": self.evidence,
            "original_sql": self.original_sql,
            "task_spec": self.task_spec.to_dict(),
            "query_spec": self.query_spec.to_dict(),
            "failure_spec": self.failure_spec.to_dict(),
            "patch_plan": self.patch_plan.to_dict(),
            "alignment_spec": self.alignment_spec.to_dict(),
        }


def build_structured_empty_sql_diagnosis(
    *,
    question_id: int,
    db_id: str,
    question: str,
    evidence: str,
    sql_text: str,
    diagnosis: Diagnosis | None,
    db_path: str | None = None,
) -> StructuredDiagnosisBundle:
    task_spec = _build_task_spec(question=question, evidence=evidence, sql_text=sql_text)
    query_spec = _build_query_spec(sql_text)
    failure_spec = _build_failure_spec(
        diagnosis=diagnosis,
        task_spec=task_spec,
        query_spec=query_spec,
        db_path=db_path,
    )
    patch_plan = _build_patch_plan(
        task_spec=task_spec,
        failure_spec=failure_spec,
        query_spec=query_spec,
    )
    alignment_spec = _build_alignment_spec(
        task_spec=task_spec,
        query_spec=query_spec,
        failure_spec=failure_spec,
        patch_plan=patch_plan,
    )
    return StructuredDiagnosisBundle(
        question_id=question_id,
        db_id=db_id,
        question=question,
        evidence=evidence,
        original_sql=sql_text,
        task_spec=task_spec,
        query_spec=query_spec,
        failure_spec=failure_spec,
        patch_plan=patch_plan,
        alignment_spec=alignment_spec,
    )


def _build_task_spec(*, question: str, evidence: str, sql_text: str) -> TaskSpec:
    question_lower = (question or "").lower()
    evidence_lower = (evidence or "").lower()
    requested_fields = sorted(
        {
            token
            for token in _FIELD_KEYWORDS
            if token in question_lower or token in evidence_lower
        }
    )
    protected_literals = _extract_protected_literals(question, evidence, sql_text)
    mandatory_constraints = sorted(
        set(_extract_comparison_constraints(question) + _extract_comparison_constraints(evidence))
    )
    target_entity_hint = _infer_target_entity_hint(question)
    answer_shape = _infer_answer_shape(question)
    aggregation_intent = _infer_aggregation_intent(question)
    return TaskSpec(
        answer_shape=answer_shape,
        target_entity_hint=target_entity_hint,
        requested_fields=requested_fields,
        protected_literals=protected_literals,
        mandatory_constraints=mandatory_constraints,
        aggregation_intent=aggregation_intent,
        boolean_intent=answer_shape == "boolean",
    )


def _build_query_spec(sql_text: str) -> QuerySpec:
    spec = QuerySpec(sql_text=sql_text or "")
    try:
        tree = parse_one(sql_text or "", read="sqlite")
    except Exception as exc:
        spec.parse_error = str(exc)
        return spec

    root_select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    cte_names = {
        (cte.alias_or_name or "").lower()
        for cte in tree.find_all(exp.CTE)
        if (cte.alias_or_name or "").strip()
    }
    alias_to_table: dict[str, str] = {}
    base_tables: list[str] = []
    for table in tree.find_all(exp.Table):
        table_name = (table.name or "").lower()
        if not table_name or table_name in cte_names:
            continue
        alias = (table.alias or "").lower()
        alias_to_table[table_name] = table_name
        if alias:
            alias_to_table[alias] = table_name
        if table_name not in base_tables:
            base_tables.append(table_name)
    spec.base_tables = base_tables

    if isinstance(tree, (exp.Union, exp.Intersect, exp.Except)):
        spec.set_operation = type(tree).__name__.lower()

    if root_select is not None:
        for select_expr in root_select.expressions or []:
            source_columns = sorted(
                {
                    _normalize_column_name(col, alias_to_table)
                    for col in select_expr.find_all(exp.Column)
                    if _normalize_column_name(col, alias_to_table)
                }
            )
            spec.projections.append(
                QueryProjection(
                    expression=select_expr.sql(dialect="sqlite"),
                    alias=(select_expr.alias or ""),
                    source_columns=source_columns,
                )
            )

        for join in root_select.find_all(exp.Join):
            table_expr = join.this
            join_table = ""
            join_alias = ""
            if isinstance(table_expr, exp.Table):
                join_table = (table_expr.name or "").lower()
                join_alias = (table_expr.alias or "").lower()
            else:
                join_table = table_expr.sql(dialect="sqlite")
            join_type = " ".join(
                part
                for part in [
                    str(join.args.get("side") or "").upper(),
                    str(join.args.get("kind") or "").upper(),
                ]
                if part
            ).strip() or "JOIN"
            on_expr = join.args.get("on")
            spec.joins.append(
                QueryJoin(
                    join_type=join_type,
                    table=join_table,
                    alias=join_alias,
                    on=on_expr.sql(dialect="sqlite") if on_expr else "",
                )
            )

        where_expr = root_select.args.get("where")
        if where_expr is not None and where_expr.this is not None:
            spec.where_predicates = [
                _predicate_from_expression(item, alias_to_table, clause="where")
                for item in _split_conjuncts(where_expr.this)
            ]

        having_expr = root_select.args.get("having")
        if having_expr is not None and having_expr.this is not None:
            spec.having_predicates = [
                _predicate_from_expression(item, alias_to_table, clause="having")
                for item in _split_conjuncts(having_expr.this)
            ]

        group_expr = root_select.args.get("group")
        if group_expr is not None:
            spec.group_by = [item.sql(dialect="sqlite") for item in group_expr.expressions or []]

        order_expr = root_select.args.get("order")
        if order_expr is not None:
            spec.order_by = [item.sql(dialect="sqlite") for item in order_expr.expressions or []]

        limit_expr = root_select.args.get("limit")
        if limit_expr is not None and limit_expr.expression is not None:
            try:
                spec.limit = int(limit_expr.expression.name)
            except Exception:
                try:
                    spec.limit = int(limit_expr.expression.sql(dialect="sqlite"))
                except Exception:
                    spec.limit = None

    subqueries: list[str] = []
    for subquery in tree.find_all(exp.Subquery):
        sql = subquery.sql(dialect="sqlite")
        if sql and sql != (sql_text or ""):
            subqueries.append(sql)
    spec.subqueries = subqueries[:10]

    literal_bindings: list[LiteralBinding] = []
    search_exprs = []
    if root_select is not None:
        for key in ("where", "having"):
            clause_expr = root_select.args.get(key)
            if clause_expr is not None and clause_expr.this is not None:
                search_exprs.append((key, clause_expr.this))
        for join in root_select.find_all(exp.Join):
            on_expr = join.args.get("on")
            if on_expr is not None:
                search_exprs.append(("join", on_expr))
    for clause, clause_expr in search_exprs:
        literal_bindings.extend(_collect_literal_bindings(clause, clause_expr, alias_to_table))
    spec.literal_bindings = literal_bindings
    return spec


def _build_failure_spec(
    *,
    diagnosis: Diagnosis | None,
    task_spec: TaskSpec,
    query_spec: QuerySpec,
    db_path: str | None,
) -> FailureSpec:
    if diagnosis is None:
        return FailureSpec(
            root_cause_type="PROBE_FAILED",
            root_cause_detail="No diagnosis object was produced.",
            evidence=["Probe did not return a diagnosis object."],
        )

    stage_trace = [
        FailureStageTrace(
            stage_idx=stage.stage_idx,
            stage_type=stage.stage_type,
            condition_text=stage.condition_text,
            row_count=stage.row_count,
            drop_from_prev=stage.drop_from_prev,
            is_killer=stage.is_killer,
        )
        for stage in diagnosis.stages
    ]
    killer = diagnosis.killer_stage
    evidence = []
    if killer is not None:
        evidence.append(
            f"Killer stage [{killer.stage_type}] `{killer.condition_text[:160]}` produced {killer.row_count} rows."
        )
    if diagnosis.root_cause_detail:
        evidence.append(diagnosis.root_cause_detail[:300])
    for probe in diagnosis.value_probes[:3]:
        actual_preview = ", ".join(str(v) for v in probe.actual_distinct_values[:5])
        evidence.append(
            f"Value probe {probe.column}: expected={probe.expected_value!r}, diagnosis={probe.diagnosis}, actual=[{actual_preview}]"
        )
    for probe in diagnosis.join_probes[:2]:
        evidence.append(
            f"Join probe {probe.left_col} vs {probe.right_col}: overlap={probe.overlap_count}, diagnosis={probe.diagnosis}"
        )

    prev_positive = None
    if killer is not None:
        for stage in diagnosis.stages:
            if stage.stage_idx >= killer.stage_idx:
                break
            if stage.row_count > 0:
                prev_positive = stage
    counterfactual_hint = ""
    if prev_positive is not None and killer is not None:
        counterfactual_hint = (
            f"Rows survive up to stage {prev_positive.stage_idx} [{prev_positive.stage_type}] "
            f"with {prev_positive.row_count} rows; the killer enters at stage {killer.stage_idx}."
        )

    value_probe_facts = [
        {
            "column": probe.column,
            "expected_value": probe.expected_value,
            "actual_distinct_values": [str(v) for v in probe.actual_distinct_values[:10]],
            "diagnosis": probe.diagnosis,
            "closest_match": probe.closest_match,
            "suggested_fix": probe.suggested_fix,
        }
        for probe in diagnosis.value_probes
    ]
    join_probe_facts = [
        {
            "left_col": probe.left_col,
            "right_col": probe.right_col,
            "left_distinct_count": probe.left_distinct_count,
            "right_distinct_count": probe.right_distinct_count,
            "overlap_count": probe.overlap_count,
            "diagnosis": probe.diagnosis,
            "suggested_fix": probe.suggested_fix,
        }
        for probe in diagnosis.join_probes
    ]
    in_query_value_hints = []
    if db_path and diagnosis.root_cause in {
        "VALUE_NOT_EXISTS",
        "EXACT_MATCH_EXISTS",
        "FUZZY_MISMATCH",
        "CASE_MISMATCH",
        "PREFIX_MATCH",
        "SUBSTRING_MATCH",
    }:
        for probe in diagnosis.value_probes[:3]:
            if not probe.expected_value:
                continue
            current_column = probe.column
            normalized_probe_column = _normalize_column_text(current_column)
            binding_columns = [
                item.column
                for item in query_spec.literal_bindings
                if item.literal == str(probe.expected_value)
            ]
            if binding_columns:
                current_column = binding_columns[0]
            hints = search_literal_in_query_tables(
                db_path=db_path,
                query_spec=query_spec.to_dict(),
                task_spec=task_spec.to_dict(),
                current_column=current_column,
                expected_literal=str(probe.expected_value),
            )
            for hint in hints[:5]:
                item = {
                    "expected_literal": str(probe.expected_value),
                    "probe_column": normalized_probe_column,
                    "current_column": _normalize_column_text(current_column),
                    **hint,
                }
                in_query_value_hints.append(item)
    date_anchor_hints = []
    anchor_swap_hints = []
    if db_path:
        date_anchor_hints = search_date_anchor_hints(
            db_path=db_path,
            task_spec=task_spec.to_dict(),
            query_spec=query_spec.to_dict(),
        )
        anchor_swap_hints = search_anchor_swap_hints(
            schema_info=None,
            database_path=db_path,
            task_spec=task_spec.to_dict(),
            query_spec=query_spec.to_dict(),
        )
    return FailureSpec(
        failure_stage=killer.stage_type if killer is not None else _infer_failure_stage(diagnosis.root_cause),
        killer_unit=killer.condition_text if killer is not None else "",
        root_cause_type=diagnosis.root_cause or "UNKNOWN",
        root_cause_detail=diagnosis.root_cause_detail or "",
        schema_anchors=list(diagnosis.schema_anchors or []),
        evidence=evidence,
        stage_trace=stage_trace,
        counterfactual_hint=counterfactual_hint,
        value_probe_facts=value_probe_facts,
        join_probe_facts=join_probe_facts,
        in_query_value_hints=in_query_value_hints,
        date_anchor_hints=date_anchor_hints,
        anchor_swap_hints=anchor_swap_hints,
    )


def _build_patch_plan(
    *,
    task_spec: TaskSpec,
    failure_spec: FailureSpec,
    query_spec: QuerySpec,
) -> PatchPlan:
    root_cause = failure_spec.root_cause_type
    protected_literals = list(task_spec.protected_literals)
    edit_targets = []
    if failure_spec.killer_unit:
        edit_targets.append(failure_spec.killer_unit)
    edit_targets.extend(failure_spec.schema_anchors[:4])

    allowed_repairs = ["edit_minimal_killer_unit_only"]
    if root_cause in {
        "FUZZY_MISMATCH",
        "CASE_MISMATCH",
        "PREFIX_MATCH",
        "SUBSTRING_MATCH",
        "VALUE_NOT_EXISTS",
        "EXACT_MATCH_EXISTS",
    }:
        allowed_repairs.extend(
            [
                "replace_literal_only_with_db_backed_value",
                "rebind_literal_to_better_column_if_probe_supports_it",
            ]
        )
    elif root_cause in {"JOIN_NO_OVERLAP", "JOIN_LOW_OVERLAP", "JOIN_PARSE_ERROR"}:
        allowed_repairs.extend(
            [
                "edit_join_key_only",
                "drop_or_replace_wrong_join_edge_if_probe_supports_it",
            ]
        )
    elif root_cause == "HAVING_TOO_RESTRICTIVE":
        allowed_repairs.extend(
            [
                "rewrite_or_remove_diagnosed_having_only",
                "move_threshold_to_where_only_if_question_has_no_group_intent",
            ]
        )
    elif root_cause in {"SUBQUERY_VALUE_MISMATCH", "SUBQUERY_RETURNS_EMPTY"}:
        allowed_repairs.extend(
            [
                "repair_subquery_predicate_only",
                "flatten_subquery_to_join_if_same_anchor",
            ]
        )
    elif root_cause in {"COMPLEX_WHERE_KILLER", "FUNCTION_FILTER_KILLS"}:
        allowed_repairs.extend(
            [
                "rewrite_diagnosed_where_predicate_only",
                "remove_unnecessary_function_wrapper_if_probe_supports_it",
            ]
        )
    else:
        allowed_repairs.append("manual_review_or_conservative_patch")

    forbidden_repairs = [
        "change_answer_shape",
        "change_requested_field_without_probe_evidence",
        "rewrite_whole_query_when_single_killer_is_known",
        "broaden_unrelated_joins",
    ]
    if query_spec.group_by and root_cause != "HAVING_TOO_RESTRICTIVE":
        forbidden_repairs.append("drop_group_by_without_explicit_reason")
    for literal in protected_literals:
        forbidden_repairs.append(f"change_literal:{literal}")

    rationale = failure_spec.counterfactual_hint or (
        f"Root cause `{root_cause}` indicates the patch should stay local to the diagnosed failure unit."
    )
    return PatchPlan(
        patch_goal=_infer_patch_goal(root_cause),
        edit_targets=edit_targets,
        allowed_repairs=allowed_repairs,
        forbidden_repairs=forbidden_repairs,
        protected_literals=protected_literals,
        answer_shape_guard=task_spec.answer_shape,
        rationale=rationale,
    )


def _build_alignment_spec(
    *,
    task_spec: TaskSpec,
    query_spec: QuerySpec,
    failure_spec: FailureSpec,
    patch_plan: PatchPlan,
) -> AlignmentSpec:
    semantic_slots = {
        "target_entity": task_spec.target_entity_hint,
        "answer_shape": task_spec.answer_shape,
        "requested_fields": list(task_spec.requested_fields),
        "protected_literals": list(task_spec.protected_literals),
        "mandatory_constraints": list(task_spec.mandatory_constraints),
        "aggregation_intent": task_spec.aggregation_intent,
        "boolean_intent": task_spec.boolean_intent,
    }

    sql_slots = {
        "target_entity_tables": list(query_spec.base_tables),
        "return_columns": [item.to_dict() for item in query_spec.projections],
        "value_predicates": [
            item.to_dict()
            for item in query_spec.where_predicates
            if item.literals and not _predicate_looks_time_like(item)
        ],
        "time_predicates": [
            item.to_dict()
            for item in query_spec.where_predicates
            if _predicate_looks_time_like(item)
        ],
        "having_predicates": [item.to_dict() for item in query_spec.having_predicates],
        "group_by": list(query_spec.group_by),
        "subqueries": list(query_spec.subqueries),
        "literal_bindings": [item.to_dict() for item in query_spec.literal_bindings],
        "date_anchor_hints": list(failure_spec.date_anchor_hints),
        "in_query_value_hints": list(failure_spec.in_query_value_hints),
        "anchor_swap_hints": list(failure_spec.anchor_swap_hints),
    }

    alignment_errors = _build_alignment_errors(
        task_spec=task_spec,
        query_spec=query_spec,
        failure_spec=failure_spec,
        patch_plan=patch_plan,
    )
    return AlignmentSpec(
        semantic_slots=semantic_slots,
        sql_slots=sql_slots,
        alignment_errors=alignment_errors,
    )


def _build_alignment_errors(
    *,
    task_spec: TaskSpec,
    query_spec: QuerySpec,
    failure_spec: FailureSpec,
    patch_plan: PatchPlan,
) -> list[AlignmentError]:
    errors: list[AlignmentError] = []
    root_cause = failure_spec.root_cause_type
    patch_goal = patch_plan.patch_goal
    killer = failure_spec.killer_unit
    evidence = failure_spec.evidence[0] if failure_spec.evidence else ""

    if patch_goal == "fix_literal_binding":
        misalignment_type = "literal_value_misaligned"
        if root_cause in {"VALUE_NOT_EXISTS", "EXACT_MATCH_EXISTS"}:
            misalignment_type = "literal_binding_slot_misaligned"
        errors.append(
            AlignmentError(
                slot="value_constraint",
                misalignment_type=misalignment_type,
                observed_sql_unit=killer,
                evidence_summary=evidence,
                suggested_actions=list(patch_plan.allowed_repairs),
            )
        )

    if patch_goal == "fix_where_predicate":
        misalignment_type = "predicate_expression_misaligned"
        if failure_spec.date_anchor_hints:
            misalignment_type = "time_anchor_or_grain_misaligned"
        errors.append(
            AlignmentError(
                slot="time_constraint" if failure_spec.date_anchor_hints else "predicate_expression",
                misalignment_type=misalignment_type,
                observed_sql_unit=killer,
                evidence_summary=evidence,
                suggested_actions=list(patch_plan.allowed_repairs),
            )
        )

    if patch_goal == "fix_subquery_alignment":
        errors.append(
            AlignmentError(
                slot="subquery_binding",
                misalignment_type="subquery_binding_slot_misaligned",
                observed_sql_unit=killer,
                evidence_summary=evidence,
                suggested_actions=list(patch_plan.allowed_repairs),
            )
        )

    if patch_goal == "fix_aggregation_filter":
        errors.append(
            AlignmentError(
                slot="aggregation_scope",
                misalignment_type="aggregation_filter_slot_misaligned",
                observed_sql_unit=killer,
                evidence_summary=evidence,
                suggested_actions=list(patch_plan.allowed_repairs),
            )
        )

    if failure_spec.failure_stage == "join":
        errors.append(
            AlignmentError(
                slot="join_path",
                misalignment_type="join_path_slot_misaligned",
                observed_sql_unit=killer,
                evidence_summary=evidence,
                suggested_actions=list(patch_plan.allowed_repairs),
            )
        )

    if failure_spec.anchor_swap_hints:
        errors.append(
            AlignmentError(
                slot="target_entity",
                misalignment_type="entity_anchor_slot_misaligned",
                observed_sql_unit=", ".join(query_spec.base_tables[:2]),
                evidence_summary=(
                    f"Question target entity hint={task_spec.target_entity_hint!r}; "
                    f"schema-backed swap candidates={len(failure_spec.anchor_swap_hints)}"
                ),
                suggested_actions=["swap_anchor_table_along_schema_path"],
            )
        )

    if failure_spec.date_anchor_hints and not any(err.slot == "time_constraint" for err in errors):
        errors.append(
            AlignmentError(
                slot="time_constraint",
                misalignment_type="time_anchor_or_grain_misaligned",
                observed_sql_unit=killer,
                evidence_summary=(
                    f"date_anchor_hints={len(failure_spec.date_anchor_hints)} candidate date locations"
                ),
                suggested_actions=["rebind_date_column", "normalize_date_grain"],
            )
        )

    if not errors:
        errors.append(
            AlignmentError(
                slot="unknown",
                misalignment_type="unclassified_empty_result_alignment",
                observed_sql_unit=killer,
                evidence_summary=evidence or failure_spec.root_cause_detail,
                suggested_actions=list(patch_plan.allowed_repairs),
            )
        )
    return errors


def _normalize_column_name(column: exp.Column, alias_to_table: dict[str, str]) -> str:
    column_name = (column.name or "").lower()
    if not column_name:
        return ""
    table_name = (column.table or "").lower()
    if table_name:
        table_name = alias_to_table.get(table_name, table_name)
        return f"{table_name}.{column_name}"
    return column_name


def _normalize_column_text(text: str) -> str:
    text = text.strip().strip("[]`\"").lower()
    if "." not in text:
        return text
    parts = [part.strip("[]`\"") for part in text.split(".")]
    return ".".join(part.lower() for part in parts if part)


def _split_conjuncts(expression: exp.Expression) -> list[exp.Expression]:
    if isinstance(expression, exp.And):
        return _split_conjuncts(expression.this) + _split_conjuncts(expression.expression)
    return [expression]


def _predicate_from_expression(
    expression: exp.Expression,
    alias_to_table: dict[str, str],
    *,
    clause: str,
) -> QueryPredicate:
    columns = sorted(
        {
            _normalize_column_name(column, alias_to_table)
            for column in expression.find_all(exp.Column)
            if _normalize_column_name(column, alias_to_table)
        }
    )
    literals = _extract_literals_from_sql(expression.sql(dialect="sqlite"))
    return QueryPredicate(
        clause=clause,
        expression=expression.sql(dialect="sqlite"),
        columns=columns,
        literals=literals,
        operator=type(expression).__name__.lower(),
    )


def _predicate_looks_time_like(predicate: QueryPredicate) -> bool:
    joined_cols = " ".join(predicate.columns).lower()
    joined_literals = " ".join(predicate.literals).lower()
    text = f"{joined_cols} {joined_literals}"
    return any(token in text for token in ("date", "time", "year", "month", "day"))


def _collect_literal_bindings(
    clause: str,
    expression: exp.Expression,
    alias_to_table: dict[str, str],
) -> list[LiteralBinding]:
    bindings: list[LiteralBinding] = []
    for node in expression.walk():
        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.ILike)):
            left = node.this
            right = node.expression
            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                bindings.append(
                    LiteralBinding(
                        clause=clause,
                        column=_normalize_column_name(left, alias_to_table),
                        operator=type(node).__name__.lower(),
                        literal=_literal_value(right),
                    )
                )
            elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                bindings.append(
                    LiteralBinding(
                        clause=clause,
                        column=_normalize_column_name(right, alias_to_table),
                        operator=type(node).__name__.lower(),
                        literal=_literal_value(left),
                    )
                )
        elif isinstance(node, exp.In) and isinstance(node.this, exp.Column):
            for item in node.expressions or []:
                if isinstance(item, exp.Literal):
                    bindings.append(
                        LiteralBinding(
                            clause=clause,
                            column=_normalize_column_name(node.this, alias_to_table),
                            operator="in",
                            literal=_literal_value(item),
                        )
                    )
        elif isinstance(node, exp.Between) and isinstance(node.this, exp.Column):
            low = node.args.get("low")
            high = node.args.get("high")
            if isinstance(low, exp.Literal):
                bindings.append(
                    LiteralBinding(
                        clause=clause,
                        column=_normalize_column_name(node.this, alias_to_table),
                        operator="between_low",
                        literal=_literal_value(low),
                    )
                )
            if isinstance(high, exp.Literal):
                bindings.append(
                    LiteralBinding(
                        clause=clause,
                        column=_normalize_column_name(node.this, alias_to_table),
                        operator="between_high",
                        literal=_literal_value(high),
                    )
                )
    return bindings


def _literal_value(literal: exp.Literal) -> str:
    return literal.this if literal.is_string else str(literal.this)


def _extract_protected_literals(question: str, evidence: str, sql_text: str) -> list[str]:
    values: set[str] = set()
    for source in (question or "", evidence or ""):
        values.update(_extract_literals_from_sql(source))
        for match in re.findall(r"\b\d{1,4}(?:[-/]\d{1,2}[-/]\d{1,4})?\b", source):
            values.add(match.strip())
        for match in re.findall(r"\b[A-Za-z]{1,8}\d+[A-Za-z0-9_-]*\b", source):
            values.add(match.strip())
    values.update(_extract_literals_from_sql(sql_text or ""))
    return sorted(
        v for v in values
        if v and not re.fullmatch(r"[A-Za-z]\d+", v)
    )


def _extract_literals_from_sql(text: str) -> list[str]:
    values: set[str] = set()
    for value in re.findall(r"""'([^']{1,160})'""", text or ""):
        values.add(value.strip())
    for value in re.findall(r'''"([^"]{1,160})"''', text or ""):
        values.add(value.strip())
    return sorted(v for v in values if v)


def _extract_comparison_constraints(text: str) -> list[str]:
    constraints = []
    for pattern in _COMPARISON_PATTERNS:
        constraints.extend(match.group(0).strip() for match in re.finditer(pattern, text or "", re.I))
    return constraints


def _infer_answer_shape(question: str) -> str:
    q = (question or "").strip().lower()
    if re.match(r"^(is|are|does|do|did|has|have|was|were|can|could|should)\b", q):
        return "boolean"
    if "how many" in q or "number of" in q or "count of" in q:
        return "scalar"
    if any(token in q for token in ["list ", "which ", "show ", "find ", "return "]):
        return "table_or_list"
    if "what are" in q and " and " in q:
        return "multi_column"
    if "what is" in q or "what are" in q:
        return "single_value"
    return "unknown"


def _infer_aggregation_intent(question: str) -> str:
    q = (question or "").lower()
    if "how many" in q or "count" in q or "number of" in q:
        return "count"
    if "average" in q or "avg" in q or "mean" in q:
        return "average"
    if "sum" in q or "total" in q:
        return "sum"
    if any(token in q for token in ["more than", "less than", "at least", "at most"]):
        return "threshold"
    if any(token in q for token in ["highest", "lowest", "top ", "most ", "least "]):
        return "ranking"
    return "none"


def _infer_target_entity_hint(question: str) -> str:
    q = " ".join((question or "").strip().split())
    patterns = [
        r"\bids?\s+of\s+the\s+([a-z_ ]{1,40}?)(?:\s+who|\s+that|\s+with|\s+where|\?)",
        r"\b([a-z_ ]{1,40}?)\s+of\s+the\s+([a-z_ ]{1,40}?)(?:\s+who|\s+that|\s+with|\s+where|\?)",
        r"\bwhich\s+([a-z_ ]{1,40}?)(?:\s+has|\s+have|\s+with|\s+where|\?)",
        r"\blist\s+(?:the\s+)?([a-z_ ]{1,40}?)(?:\s+with|\s+where|\s+for|\?)",
        r"\bwhat\s+(?:is|are)\s+the\s+([a-z_ ]{1,40}?)(?:\s+of|\s+for|\s+with|\?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, q, re.I)
        if match:
            if match.lastindex and match.lastindex >= 2:
                return match.group(2).strip()
            return match.group(1).strip()
    return ""


def _infer_failure_stage(root_cause: str) -> str:
    if root_cause.startswith("JOIN_"):
        return "join"
    if root_cause.startswith("SET_OP_"):
        return "branch"
    if root_cause == "HAVING_TOO_RESTRICTIVE":
        return "having"
    if root_cause in {"SUBQUERY_VALUE_MISMATCH", "SUBQUERY_RETURNS_EMPTY"}:
        return "subquery"
    if root_cause in {
        "FUZZY_MISMATCH",
        "CASE_MISMATCH",
        "PREFIX_MATCH",
        "SUBSTRING_MATCH",
        "VALUE_NOT_EXISTS",
        "EXACT_MATCH_EXISTS",
        "COMPLEX_WHERE_KILLER",
        "FUNCTION_FILTER_KILLS",
    }:
        return "where"
    return "unknown"


def _infer_patch_goal(root_cause: str) -> str:
    if root_cause in {
        "FUZZY_MISMATCH",
        "CASE_MISMATCH",
        "PREFIX_MATCH",
        "SUBSTRING_MATCH",
        "VALUE_NOT_EXISTS",
        "EXACT_MATCH_EXISTS",
    }:
        return "fix_literal_binding"
    if root_cause.startswith("JOIN_"):
        return "fix_join_anchor"
    if root_cause == "HAVING_TOO_RESTRICTIVE":
        return "fix_aggregation_filter"
    if root_cause in {"SUBQUERY_VALUE_MISMATCH", "SUBQUERY_RETURNS_EMPTY"}:
        return "fix_subquery_alignment"
    if root_cause in {"COMPLEX_WHERE_KILLER", "FUNCTION_FILTER_KILLS"}:
        return "fix_where_predicate"
    return "conservative_manual_patch"
