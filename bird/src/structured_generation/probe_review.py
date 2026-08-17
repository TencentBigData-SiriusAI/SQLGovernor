"""Probe-based grounded review for structured SQL candidates."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import re
import sqlite3
import time
from typing import Any

from openai import OpenAI

from .llm_retry import chat_completion_with_retry
from ..tools.sql_validator import validate_sql_schema, validate_sql_syntax
from ..utils.sql_text import clean_sql_output
from .schema_graph import SchemaGraph
from .types import PathPlan, StructuredCandidate, StructuredPlan

_PROBE_SYSTEM_PROMPT = (
    "You are a database disambiguation planner for text-to-SQL. "
    "Given a question, evidence, full schema, selected path, and a draft SQL query, "
    "identify a few cheap diagnostic probes that can clarify remaining ambiguity. "
    "Do not search for the final answer directly. Return JSON only."
)

_SCOPE_PROFILE_PROMPT_SECTION = """
5. scope_profile
{{
  "probe_type": "scope_profile",
  "from_table": "...",
  "joins": [{{"table": "...", "on": "..."}}],
  "measure_aggregate": "COUNT|AVG|SUM|MIN|MAX",
  "measure_column": "* or ...",
  "where_clauses": ["..."],
  "focus_where_clauses": ["..."],
  "group_by_columns": ["..."],
  "limit": 5,
  "reason": "..."
}}
"""

_SCOPE_PROFILE_PROMPT_RULES = """
- For `scope_profile`, choose exactly one mode:
  compare mode -> use `focus_where_clauses` to compare a narrower slice against the base `where_clauses`
  grouped mode -> use `group_by_columns` to inspect per-slice metrics
- Only use `scope_profile` when the ambiguity is about global vs subset scope, time/date scope, or group-then-rank scope.
- Do not set both `focus_where_clauses` and `group_by_columns`.
- Use `measure_column`="*" only with COUNT.
"""

_PROBE_PROMPT = """Database Engine:
SQLite

Full Database Schema:
{schema}

Schema Graph Summary:
{graph_summary}

Question:
{question}

Evidence:
{evidence}

Answer Intent:
{answer_intent}

Selected Path:
{path_payload}

Draft SQL:
{sql}

Task:
Return valid JSON with key `probes`, where each probe is one of:

1. value_distribution
{{
  "probe_type": "value_distribution",
  "target_table": "...",
  "target_column": "...",
  "where_clauses": ["..."],
  "limit": 20,
  "reason": "..."
}}

2. column_pair_samples
{{
  "probe_type": "column_pair_samples",
  "target_table": "...",
  "target_columns": ["...", "..."],
  "where_clauses": ["..."],
  "limit": 10,
  "reason": "..."
}}

3. join_coverage
{{
  "probe_type": "join_coverage",
  "from_table": "...",
  "joins": [{{"table": "...", "on": "..."}}],
  "where_clauses": ["..."],
  "distinct_columns": ["..."],
  "reason": "..."
}}

4. granularity_profile
{{
  "probe_type": "granularity_profile",
  "target_table": "...",
  "distinct_columns": ["..."],
  "where_clauses": ["..."],
  "reason": "..."
}}

{scope_profile_section}

Rules:
- Return at most {max_probes} probes.
- Only use cheap diagnostic probes. Prefer DISTINCT/LIMIT or COUNT-based probes.
- Do not query the final answer directly.
- Do not use subqueries, CTEs, ORDER BY, or more than 3 joined tables in a probe.
{scope_profile_rules}
- Use full schema for disambiguation.
- Do not output markdown.
"""

_GROUNDED_REVIEW_SYSTEM_PROMPT = (
    "You are a careful SQL reviewer. "
    "Use the probe findings as grounded evidence to minimally revise the draft SQL only when needed. "
    "Be conservative: preserve the original SQL unless the probe findings clearly indicate a likely ambiguity or mismatch."
)

_GROUNDED_REVIEW_PROMPT = """Database Engine:
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

Draft SQL:
{sql}

Probe Findings:
{probe_findings}

Task:
Return valid JSON with keys:
- grounded_review_issues: list[str]
- should_rewrite: bool
- corrected_sql: string

Rules:
- Only rewrite if the probe findings provide concrete evidence that the draft SQL likely uses the wrong field type, wrong join path, wrong granularity, or wrong scope.
- Make the smallest possible change.
- Preserve the answer shape unless the probe findings clearly show the output columns are mismatched.
- If uncertain, keep the draft SQL.
- If no rewrite is needed, set should_rewrite=false and corrected_sql="".
- Do not output markdown.
"""


@dataclass(slots=True)
class ProbeJoin:
    table: str
    on: str

    @classmethod
    def from_dict(cls, payload: Any) -> "ProbeJoin | None":
        if not isinstance(payload, dict):
            return None
        table = str(payload.get("table", "")).strip()
        on = str(payload.get("on", "")).strip()
        if not table or not on:
            return None
        return cls(table=table, on=on)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ProbeSpec:
    probe_type: str
    target_table: str = ""
    target_column: str = ""
    target_columns: list[str] = field(default_factory=list)
    from_table: str = ""
    joins: list[ProbeJoin] = field(default_factory=list)
    where_clauses: list[str] = field(default_factory=list)
    focus_where_clauses: list[str] = field(default_factory=list)
    distinct_columns: list[str] = field(default_factory=list)
    group_by_columns: list[str] = field(default_factory=list)
    measure_aggregate: str = ""
    measure_column: str = ""
    limit: int = 10
    reason: str = ""

    @classmethod
    def from_dict(cls, payload: Any) -> "ProbeSpec | None":
        if not isinstance(payload, dict):
            return None
        probe_type = str(payload.get("probe_type", "")).strip()
        if not probe_type:
            return None
        joins = []
        for item in payload.get("joins") or []:
            parsed = ProbeJoin.from_dict(item)
            if parsed is not None:
                joins.append(parsed)
        limit = payload.get("limit", 10)
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 10
        return cls(
            probe_type=probe_type,
            target_table=str(payload.get("target_table", "")).strip(),
            target_column=str(payload.get("target_column", "")).strip(),
            target_columns=[
                str(item).strip()
                for item in payload.get("target_columns") or []
                if str(item).strip()
            ],
            from_table=str(payload.get("from_table", "")).strip(),
            joins=joins,
            where_clauses=[
                str(item).strip()
                for item in payload.get("where_clauses") or []
                if str(item).strip()
            ],
            focus_where_clauses=[
                str(item).strip()
                for item in payload.get("focus_where_clauses") or []
                if str(item).strip()
            ],
            distinct_columns=[
                str(item).strip()
                for item in payload.get("distinct_columns") or []
                if str(item).strip()
            ],
            group_by_columns=[
                str(item).strip()
                for item in payload.get("group_by_columns") or []
                if str(item).strip()
            ],
            measure_aggregate=str(payload.get("measure_aggregate", "")).strip().upper(),
            measure_column=str(payload.get("measure_column", "")).strip(),
            limit=max(1, min(limit, 20)),
            reason=str(payload.get("reason", "")).strip(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "probe_type": self.probe_type,
            "target_table": self.target_table,
            "target_column": self.target_column,
            "target_columns": self.target_columns,
            "from_table": self.from_table,
            "joins": [item.to_dict() for item in self.joins],
            "where_clauses": self.where_clauses,
            "focus_where_clauses": self.focus_where_clauses,
            "distinct_columns": self.distinct_columns,
            "group_by_columns": self.group_by_columns,
            "measure_aggregate": self.measure_aggregate,
            "measure_column": self.measure_column,
            "limit": self.limit,
            "reason": self.reason,
        }


def apply_probe_grounded_review(
    candidate: StructuredCandidate,
    *,
    client: OpenAI,
    model_name: str,
    question: str,
    evidence: str,
    schema_description: str,
    graph: SchemaGraph,
    plan: StructuredPlan,
    path: PathPlan,
    schema_info: dict[str, Any],
    database_path: str,
    max_probes: int = 2,
    enable_scope_probe: bool = False,
    probe_planner_temperature: float = 0.2,
    probe_review_temperature: float = 0.2,
    probe_timeout: int = 5,
    max_tokens: int = 3072,
    timeout: int = 120,
) -> StructuredCandidate:
    probe_specs = generate_probe_specs(
        client=client,
        model_name=model_name,
        question=question,
        evidence=evidence,
        schema=schema_description,
        graph=graph,
        plan=plan,
        path=path,
        candidate=candidate,
        max_probes=max_probes,
        enable_scope_probe=enable_scope_probe,
        temperature=probe_planner_temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    candidate.probe_specs = [item.to_dict() for item in probe_specs]

    findings: list[dict[str, Any]] = []
    for probe_spec in probe_specs:
        sql_text = render_probe_sql(probe_spec)
        if not sql_text:
            continue
        execution = execute_probe_sql(
            database_path=database_path, sql_text=sql_text, timeout=probe_timeout
        )
        findings.append(summarize_probe_finding(probe_spec, sql_text, execution))
    candidate.probe_findings = findings

    if not findings:
        return candidate

    response = chat_completion_with_retry(
        client,
        model=model_name,
        messages=[
            {"role": "system", "content": _GROUNDED_REVIEW_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": _GROUNDED_REVIEW_PROMPT.format(
                    schema=schema_description,
                    question=question,
                    evidence=evidence or "(none)",
                    answer_intent=plan.answer_intent.to_dict() if plan.answer_intent else "(none)",
                    path_payload=path.to_dict(),
                    sql=candidate.sql,
                    probe_findings=json.dumps(findings, ensure_ascii=False, indent=2),
                ),
            },
        ],
        temperature=probe_review_temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    raw_content = response.choices[0].message.content or ""
    payload = _extract_json_payload(raw_content)
    issues = [str(item) for item in payload.get("grounded_review_issues", [])]
    candidate.probe_review_issues = issues
    should_rewrite = bool(payload.get("should_rewrite", False))
    corrected_sql = clean_sql_output(str(payload.get("corrected_sql", "")))
    if not should_rewrite or not corrected_sql.strip():
        return candidate

    if not candidate.original_sql:
        candidate.original_sql = candidate.sql
    candidate.sql = corrected_sql.strip()
    candidate.auto_correct_attempts += 1
    syntax_errors = validate_sql_syntax(candidate.sql) if candidate.sql else ["EMPTY_SQL"]
    schema_errors = (
        validate_sql_schema(candidate.sql, schema_info, database_path) if not syntax_errors else []
    )
    candidate.errors = syntax_errors + schema_errors
    candidate.is_valid = not candidate.errors
    candidate.correction_history.append(
        {
            "attempt": candidate.auto_correct_attempts,
            "mode": "probe_grounded_review",
            "probe_review_issues": issues,
            "corrected_sql": candidate.sql,
            "probe_findings": findings,
        }
    )
    return candidate


def generate_probe_specs(
    *,
    client: OpenAI,
    model_name: str,
    question: str,
    evidence: str,
    schema: str,
    graph: SchemaGraph,
    plan: StructuredPlan,
    path: PathPlan,
    candidate: StructuredCandidate,
    max_probes: int = 2,
    enable_scope_probe: bool = False,
    temperature: float = 0.2,
    max_tokens: int = 2048,
    timeout: int = 120,
) -> list[ProbeSpec]:
    prompt = _PROBE_PROMPT.format(
        schema=schema,
        graph_summary=graph.summarize(),
        question=question,
        evidence=evidence or "(none)",
        answer_intent=plan.answer_intent.to_dict() if plan.answer_intent else "(none)",
        path_payload=path.to_dict(),
        sql=candidate.sql,
        max_probes=max_probes,
        scope_profile_section=_SCOPE_PROFILE_PROMPT_SECTION.strip() if enable_scope_probe else "",
        scope_profile_rules=_SCOPE_PROFILE_PROMPT_RULES.strip() if enable_scope_probe else "",
    )
    response = chat_completion_with_retry(
        client,
        model=model_name,
        messages=[
            {"role": "system", "content": _PROBE_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    raw_content = response.choices[0].message.content or ""
    payload = _extract_json_payload(raw_content)
    items = payload.get("probes") or []
    if isinstance(items, dict):
        items = list(items.values())
    results: list[ProbeSpec] = []
    for item in items[:max_probes]:
        probe = ProbeSpec.from_dict(item)
        if probe is None:
            continue
        if probe.probe_type == "scope_profile" and not enable_scope_probe:
            continue
        if render_probe_sql(probe):
            results.append(probe)
    return results


def render_probe_sql(spec: ProbeSpec) -> str:
    if spec.probe_type == "value_distribution":
        if not spec.target_table or not spec.target_column:
            return ""
        target_table = _quote_table_if_needed(spec.target_table)
        target_column = _quote_identifier_if_needed(spec.target_column)
        where_clauses = _quote_unusual_names_in_clauses(
            [f"{spec.target_column} IS NOT NULL"] + list(spec.where_clauses),
            spec,
        )
        return (
            f"SELECT DISTINCT {target_column} "
            f"FROM {target_table} "
            f"{_render_where(where_clauses)} "
            f"LIMIT {max(1, min(spec.limit, 20))};"
        )

    if spec.probe_type == "column_pair_samples":
        if not spec.target_table or len(spec.target_columns) < 2:
            return ""
        col1, col2 = spec.target_columns[:2]
        target_table = _quote_table_if_needed(spec.target_table)
        qcol1 = _quote_identifier_if_needed(col1)
        qcol2 = _quote_identifier_if_needed(col2)
        where_clauses = _quote_unusual_names_in_clauses(
            [f"{col1} IS NOT NULL"] + list(spec.where_clauses),
            spec,
        )
        return (
            f"SELECT {qcol1}, {qcol2} "
            f"FROM {target_table} "
            f"{_render_where(where_clauses)} "
            f"LIMIT {max(1, min(spec.limit, 20))};"
        )

    if spec.probe_type == "join_coverage":
        if not spec.from_table:
            return ""
        parts = [f"SELECT COUNT(*) AS row_count"]
        for column in spec.distinct_columns[:2]:
            alias = _alias_from_column(column)
            parts.append(
                f", COUNT(DISTINCT {_quote_identifier_if_needed(column)}) AS distinct_{alias}"
            )
        parts.append(f" FROM {_quote_table_if_needed(spec.from_table)}")
        join_count = 0
        for join in spec.joins[:2]:
            on_clause = _quote_unusual_names_in_clause(join.on, spec)
            parts.append(f" INNER JOIN {_quote_table_if_needed(join.table)} ON {on_clause}")
            join_count += 1
        if join_count == 0:
            return ""
        where_sql = _render_where(_quote_unusual_names_in_clauses(spec.where_clauses, spec))
        if where_sql:
            parts.append(f" {where_sql}")
        parts.append(";")
        return "".join(parts)

    if spec.probe_type == "granularity_profile":
        if not spec.target_table:
            return ""
        parts = [f"SELECT COUNT(*) AS row_count"]
        for column in spec.distinct_columns[:2]:
            alias = _alias_from_column(column)
            parts.append(
                f", COUNT(DISTINCT {_quote_identifier_if_needed(column)}) AS distinct_{alias}"
            )
        parts.append(f" FROM {_quote_table_if_needed(spec.target_table)}")
        where_sql = _render_where(_quote_unusual_names_in_clauses(spec.where_clauses, spec))
        if where_sql:
            parts.append(f" {where_sql}")
        parts.append(";")
        return "".join(parts)

    if spec.probe_type == "scope_profile":
        metric_sql = _render_scope_metric(spec)
        if not spec.from_table or not metric_sql:
            return ""
        from_sql = _render_from_with_joins(spec)
        if not from_sql:
            return ""

        has_focus_scope = bool(spec.focus_where_clauses)
        has_grouped_scope = bool(spec.group_by_columns)
        if has_focus_scope == has_grouped_scope:
            return ""

        base_where = _quote_unusual_names_in_clauses(spec.where_clauses, spec)
        focus_where = _quote_unusual_names_in_clauses(spec.focus_where_clauses, spec)
        group_by_columns = [
            _quote_identifier_if_needed(column)
            for column in spec.group_by_columns[:2]
            if column and column.strip()
        ]

        if focus_where:
            base_sql = (
                f"SELECT 'base' AS scope_label, {metric_sql} AS metric_value"
                f" {from_sql} {_render_where(base_where)}"
            ).strip()
            focused_sql = (
                f"SELECT 'focused' AS scope_label, {metric_sql} AS metric_value"
                f" {from_sql} {_render_where(base_where + focus_where)}"
            ).strip()
            return f"{base_sql} UNION ALL {focused_sql};"

        if not group_by_columns:
            return ""

        select_columns = ", ".join(group_by_columns + [f"{metric_sql} AS metric_value"])
        group_by_sql = ", ".join(group_by_columns)
        where_sql = _render_where(base_where)
        return (
            f"SELECT {select_columns} {from_sql} {where_sql} "
            f"GROUP BY {group_by_sql} LIMIT {max(1, min(spec.limit, 20))};"
        ).strip()

    return ""


def execute_probe_sql(*, database_path: str, sql_text: str, timeout: int) -> dict[str, Any]:
    conn = sqlite3.connect(database_path)
    start = time.perf_counter()

    def _progress_handler() -> None:
        if timeout > 0 and time.perf_counter() - start > timeout:
            raise TimeoutError("Probe execution timed out")

    try:
        if timeout > 0:
            conn.set_progress_handler(_progress_handler, 1000)
        cursor = conn.cursor()
        cursor.execute(sql_text)
        rows = cursor.fetchall()
        return {
            "status": "succeeded",
            "success": True,
            "error": None,
            "elapsed": time.perf_counter() - start,
            "row_count": len(rows),
            "rows": rows[:20],
        }
    except TimeoutError as exc:
        return {
            "status": "timeout",
            "success": False,
            "error": str(exc),
            "elapsed": time.perf_counter() - start,
            "row_count": 0,
            "rows": [],
        }
    except Exception as exc:
        return {
            "status": "failed",
            "success": False,
            "error": str(exc),
            "elapsed": time.perf_counter() - start,
            "row_count": 0,
            "rows": [],
        }
    finally:
        conn.set_progress_handler(None, 0)
        conn.close()


def summarize_probe_finding(
    spec: ProbeSpec, sql_text: str, execution: dict[str, Any]
) -> dict[str, Any]:
    rows = execution.get("rows") or []
    finding = {
        "probe_type": spec.probe_type,
        "reason": spec.reason,
        "sql": sql_text,
        "execution": execution,
        "summary": "",
    }
    if not execution.get("success"):
        finding["summary"] = f"Probe failed: {execution.get('error')}"
        return finding

    if spec.probe_type == "value_distribution":
        samples = [row[0] for row in rows if row]
        finding["summary"] = (
            f"Observed {len(samples)} distinct values for {spec.target_column}: "
            f"{_summarize_value_style(samples)}"
        )
        return finding

    if spec.probe_type == "column_pair_samples":
        first_samples = [row[0] for row in rows if len(row) > 0]
        second_samples = [row[1] for row in rows if len(row) > 1]
        finding["summary"] = (
            f"Observed paired samples. {spec.target_columns[0]} looks {_summarize_value_style(first_samples)}; "
            f"{spec.target_columns[1]} looks {_summarize_value_style(second_samples)}"
        )
        return finding

    if spec.probe_type in {"join_coverage", "granularity_profile"}:
        if rows:
            row = rows[0]
            finding["summary"] = f"Coverage/profile row: {list(row)}"
        else:
            finding["summary"] = "Probe returned no rows."
        return finding

    if spec.probe_type == "scope_profile":
        if not rows:
            finding["summary"] = "Scope profile returned no rows."
            return finding
        if spec.focus_where_clauses:
            compare_rows = [list(row[:2]) for row in rows[:2]]
            finding["summary"] = f"Scope comparison rows: {compare_rows}"
            return finding
        grouping = spec.group_by_columns[:2]
        finding["summary"] = (
            f"Grouped scope profile on {grouping}: {[list(row) for row in rows[:5]]}"
        )
        return finding

    finding["summary"] = "Probe executed."
    return finding


def _render_scope_metric(spec: ProbeSpec) -> str:
    aggregate = spec.measure_aggregate.strip().upper()
    if aggregate not in {"COUNT", "AVG", "SUM", "MIN", "MAX"}:
        return ""
    column = spec.measure_column.strip() or "*"
    if column == "*":
        return "COUNT(*)" if aggregate == "COUNT" else ""
    return f"{aggregate}({_quote_identifier_if_needed(column)})"


def _render_from_with_joins(spec: ProbeSpec) -> str:
    if not spec.from_table:
        return ""
    parts = [f"FROM {_quote_table_if_needed(spec.from_table)}"]
    for join in spec.joins[:2]:
        on_clause = _quote_unusual_names_in_clause(join.on, spec)
        parts.append(f"INNER JOIN {_quote_table_if_needed(join.table)} ON {on_clause}")
    return " ".join(parts)


def _render_where(clauses: list[str]) -> str:
    cleaned = [clause.strip() for clause in clauses if clause and clause.strip()]
    if not cleaned:
        return ""
    return "WHERE " + " AND ".join(cleaned)


def _alias_from_column(column: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_]+", "_", column.split(".")[-1]).strip("_").lower()
    return token or "value"


def _summarize_value_style(samples: list[Any]) -> str:
    normalized = [str(item) for item in samples[:10] if item is not None]
    if not normalized:
        return "empty"
    numeric_like = sum(_is_numeric_token(item) for item in normalized)
    if numeric_like == len(normalized):
        return f"numeric-like values {normalized[:5]}"
    short_upper = sum(bool(re.fullmatch(r"[A-Z0-9_-]{1,8}", item)) for item in normalized)
    if short_upper >= max(1, len(normalized) // 2):
        return f"code-like values {normalized[:5]}"
    return f"description-like values {normalized[:5]}"


def _is_numeric_token(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def _quote_unusual_names_in_clauses(clauses: list[str], spec: ProbeSpec) -> list[str]:
    return [_quote_unusual_names_in_clause(clause, spec) for clause in clauses]


def _quote_unusual_names_in_clause(clause: str, spec: ProbeSpec) -> str:
    result = clause
    for raw_name in sorted(_collect_unusual_names(spec), key=len, reverse=True):
        quoted = (
            _quote_identifier_if_needed(raw_name)
            if "." in raw_name
            else _quote_name_if_needed(raw_name)
        )
        result = result.replace(raw_name, quoted)
    return result


def _collect_unusual_names(spec: ProbeSpec) -> list[str]:
    names: set[str] = set()
    for item in [
        spec.target_table,
        spec.target_column,
        spec.from_table,
        spec.measure_column,
        *spec.target_columns,
        *spec.distinct_columns,
        *spec.group_by_columns,
        *(join.table for join in spec.joins),
    ]:
        if item in {"", "*"}:
            continue
        if item and _needs_quoting(item):
            names.add(item)
        if "." in item:
            for part in item.split("."):
                if part and _needs_quoting(part):
                    names.add(part)
    return list(names)


def _quote_table_if_needed(identifier: str) -> str:
    return (
        ".".join(_quote_name_if_needed(part) for part in identifier.split("."))
        if identifier
        else identifier
    )


def _quote_identifier_if_needed(identifier: str) -> str:
    return (
        ".".join(_quote_name_if_needed(part) for part in identifier.split("."))
        if identifier
        else identifier
    )


def _quote_name_if_needed(name: str) -> str:
    stripped = name.strip()
    if stripped.startswith("`") and stripped.endswith("`"):
        return stripped
    return f"`{stripped}`" if _needs_quoting(stripped) else stripped


def _needs_quoting(name: str) -> bool:
    return not bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name))


def _extract_json_payload(text: str) -> dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S)
    if fenced:
        text = fenced.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise ValueError("Probe review response did not contain a JSON object")
    payload = text[start : end + 1]
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        repaired = re.sub(r'\\(?!["\\\\/bfnrtu])', r"\\\\", payload)
        return json.loads(repaired)
