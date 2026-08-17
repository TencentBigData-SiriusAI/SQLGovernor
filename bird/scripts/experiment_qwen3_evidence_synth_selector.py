#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from loguru import logger
from pathlib import Path
from typing import Any

from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.model_config import get_model_config
from error_bank.diagnosis_spec import _build_query_spec, _build_task_spec
from src.tools.sql_result_voter import canonicalize_rows


CHOICE_PROMPT = """# Role
You are an expert SQL candidate judge.

# Goal
You are given a database question, evidence, a schema summary, and a synthesis of many SQL exploration candidates.
Your task is to choose the candidate cluster whose SQL logic is most likely to answer the question correctly.

# Important rules
1. Prefer semantic correctness over cluster size.
2. Use the synthesis summary to identify consensus and conflicts across the explored SQLs.
3. Treat execution result previews as supporting evidence, not the only signal.
4. Be suspicious of clusters that:
   - return the wrong entity/field,
   - use the wrong table anchor,
   - use the wrong time grain,
   - simplify away a required condition.
5. Prefer the minimally sufficient SQL. Extra aggregation, extra ranking/window functions, extra projected columns, or extra CTE layers should lose unless the question/evidence explicitly requires them.
6. Do not over-infer hidden requirements. If the question/evidence do not clearly require SUM/AVG/windowing or extra columns, avoid rewarding them.
7. If two clusters are close, prefer the one that better matches the question/evidence constraints and answer shape.

# Output
Return JSON only:
{{"chosen_cluster_id": "C1", "reason": "..." }}

# Input
Question:
{question}

Evidence:
{evidence}

Schema:
{schema_text}

Task summary:
{task_summary}

Evidence synthesis:
{evidence_synthesis}

Candidate clusters:
{cluster_blocks}
"""


@dataclass
class CandidateExec:
    candidate_index: int
    sql: str
    source: str
    vote_count: int
    row_count: int | None
    col_count: int | None
    rows_preview: list[list[Any]]
    rows_signature: frozenset[tuple[Any, ...]] | None
    success: bool
    error: str | None
    query_spec: Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Qwen3 evidence-synthesis selector pilot.")
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--pass-report", type=Path)
    parser.add_argument("--qid-list", type=Path, help="Optional newline-delimited qids to evaluate directly")
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--gold", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model-key", type=str, default="qwen3-235b")
    parser.add_argument("--max-samples", type=int, default=10)
    parser.add_argument("--max-candidates", type=int, default=32)
    parser.add_argument("--max-clusters", type=int, default=6)
    parser.add_argument("--max-preview-rows", type=int, default=5)
    parser.add_argument("--max-schema-chars", type=int, default=4000)
    parser.add_argument("--max-cluster-blocks-chars", type=int, default=12000)
    parser.add_argument("--max-rows-preview-chars-per-cluster", type=int, default=1500)
    parser.add_argument(
        "--cluster-prompt-style",
        choices=["compact", "full"],
        default="compact",
        help="How much detail to include for each candidate cluster in the selector prompt.",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--response-max-tokens", type=int, default=2048)
    parser.add_argument("--group-mode", choices=["result", "hypothesis"], default="result")
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--drop-empty", action="store_true", help="Exclude candidates whose stored execution is empty_result before synthesis")
    parser.add_argument(
        "--dry-run-prompts",
        action="store_true",
        help="Build prompt diagnostics only; do not call the qwen selector model.",
    )
    return parser.parse_args()


def parse_major_miss_blocks(text: str) -> list[dict[str, Any]]:
    pattern = re.compile(
        r"question_id=(?P<qid>\d+)\s+db_id=(?P<db_id>[^\s]+)\s+pass@k_position=(?P<pos>\d+)\s+candidate_index=(?P<cand_idx>\d+)",
        re.M,
    )
    matches = list(pattern.finditer(text))
    blocks = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        block = text[start:end]
        major_sql = _extract_labeled_sql(block, "major_voting_sql:")
        pass_sql = _extract_labeled_sql(block, "pass@k_sql:")
        blocks.append(
            {
                "question_id": int(match.group("qid")),
                "db_id": match.group("db_id"),
                "pass_position": int(match.group("pos")),
                "candidate_index": int(match.group("cand_idx")),
                "major_sql": major_sql,
                "pass_sql": pass_sql,
            }
        )
    return blocks


def _extract_labeled_sql(block: str, label: str) -> str:
    marker = block.find(label)
    if marker < 0:
        return ""
    remainder = block[marker + len(label):]
    stop = remainder.find("================================================================================")
    if stop >= 0:
        remainder = remainder[:stop]
    next_label_positions = [
        pos for pos in [
            remainder.find("major_voting_sql:"),
            remainder.find("pass@k_sql:"),
        ] if pos >= 0
    ]
    if next_label_positions:
        first = min(next_label_positions)
        remainder = remainder[:first]
    return remainder.strip()


def execute_candidate(
    *,
    db_path: str,
    sql: str,
    timeout: int,
    max_preview_rows: int,
    candidate_index: int,
    source: str,
    vote_count: int,
) -> CandidateExec:
    conn = sqlite3.connect(db_path, timeout=5.0)
    start = sqlite3.Connection.total_changes if False else None  # keep linter quiet

    import time
    started = time.time()

    def _progress_handler() -> None:
        if timeout > 0 and time.time() - started > timeout:
            raise TimeoutError("SQL execution timed out")

    try:
        if timeout > 0:
            conn.set_progress_handler(_progress_handler, 1000)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        query_spec = _build_query_spec(sql)
        return CandidateExec(
            candidate_index=candidate_index,
            sql=sql,
            source=source,
            vote_count=vote_count,
            row_count=len(rows),
            col_count=len(columns),
            rows_preview=[list(row) for row in rows[:max_preview_rows]],
            rows_signature=canonicalize_rows(rows),
            success=True,
            error=None,
            query_spec=query_spec,
        )
    except Exception as exc:
        return CandidateExec(
            candidate_index=candidate_index,
            sql=sql,
            source=source,
            vote_count=vote_count,
            row_count=None,
            col_count=None,
            rows_preview=[],
            rows_signature=None,
            success=False,
            error=str(exc),
            query_spec=_build_query_spec(sql),
        )
    finally:
        try:
            conn.set_progress_handler(None, 0)
        except Exception:
            pass
        conn.close()


def summarize_task(question: str, evidence: str) -> dict[str, Any]:
    task = _build_task_spec(question=question, evidence=evidence, sql_text="")
    return task.to_dict()


def cluster_candidates(records: list[CandidateExec]) -> list[dict[str, Any]]:
    clusters: dict[Any, list[CandidateExec]] = defaultdict(list)
    for record in records:
        if record.success and record.rows_signature is not None:
            clusters[record.rows_signature].append(record)

    cluster_rows = []
    for idx, (_, members) in enumerate(
        sorted(clusters.items(), key=lambda item: (-len(item[1]), -max(m.vote_count for m in item[1])))
    ):
        rep = sorted(
            members,
            key=lambda item: (-item.vote_count, item.candidate_index),
        )[0]
        table_sets = Counter(tuple(rep.query_spec.base_tables))
        projection_patterns = Counter(
            tuple(col for proj in item.query_spec.projections for col in proj.source_columns)
            for item in members
        )
        rep_logic = summarize_query_logic(rep.query_spec)
        cluster_rows.append(
            {
                "cluster_id": f"C{idx+1}",
                "size": len(members),
                "representative_candidate_index": rep.candidate_index,
                "representative_sql": rep.sql,
                "row_count": rep.row_count,
                "col_count": rep.col_count,
                "rows_preview": rep.rows_preview,
                "source_summary": Counter(item.source for item in members).most_common(3),
                "vote_count_max": max(item.vote_count for item in members),
                "tables_counter": Counter(tuple(item.query_spec.base_tables) for item in members).most_common(3),
                "projection_counter": projection_patterns.most_common(3),
                "logic_summary": rep_logic,
                "rep_base_tables": list(rep.query_spec.base_tables),
                "members": members,
            }
        )
    return cluster_rows


def group_hypotheses(records: list[CandidateExec]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[CandidateExec]] = defaultdict(list)
    for record in records:
        if not record.success:
            continue
        logic = summarize_query_logic(record.query_spec)
        key = (
            tuple(record.query_spec.base_tables),
            tuple(logic["projected_columns"]),
            tuple(logic["normalized_time_predicates"]),
            tuple(logic["normalized_value_predicates"]),
            logic["has_aggregate"],
            logic["has_group_by"],
            logic["has_window"],
            logic["has_cte"],
        )
        groups[key].append(record)

    rows = []
    for idx, (_, members) in enumerate(
        sorted(groups.items(), key=lambda item: (-len(item[1]), -max(m.vote_count for m in item[1])))
    ):
        rep = sorted(members, key=lambda item: (-item.vote_count, item.candidate_index))[0]
        logic = summarize_query_logic(rep.query_spec)
        rows.append(
            {
                "cluster_id": f"H{idx+1}",
                "size": len(members),
                "representative_candidate_index": rep.candidate_index,
                "representative_sql": rep.sql,
                "row_count": rep.row_count,
                "col_count": rep.col_count,
                "rows_preview": rep.rows_preview,
                "source_summary": Counter(item.source for item in members).most_common(3),
                "vote_count_max": max(item.vote_count for item in members),
                "tables_counter": Counter(tuple(item.query_spec.base_tables) for item in members).most_common(3),
                "projection_counter": Counter(
                    tuple(col for proj in item.query_spec.projections for col in proj.source_columns)
                    for item in members
                ).most_common(3),
                "logic_summary": logic,
                "rep_base_tables": list(rep.query_spec.base_tables),
                "members": members,
            }
        )
    return rows


def synthesize_evidence(question: str, evidence: str, records: list[CandidateExec], clusters: list[dict[str, Any]]) -> str:
    table_counter = Counter(tuple(record.query_spec.base_tables) for record in records if record.success)
    projection_counter = Counter(
        tuple(col for proj in record.query_spec.projections for col in proj.source_columns)
        for record in records if record.success
    )
    time_pred_counter = Counter(
        tuple(pred.expression for pred in record.query_spec.where_predicates if any("date" in col or "time" in col for col in pred.columns))
        for record in records if record.success
    )
    value_pred_counter = Counter(
        tuple(pred.expression for pred in record.query_spec.where_predicates if pred.literals)
        for record in records if record.success
    )
    complexity_counter = Counter(
        (
            summarize_query_logic(record.query_spec)["projected_column_count"],
            summarize_query_logic(record.query_spec)["has_aggregate"],
            summarize_query_logic(record.query_spec)["has_window"],
            summarize_query_logic(record.query_spec)["has_cte"],
        )
        for record in records if record.success
    )

    lines = []
    lines.append(f"- successful_candidates: {sum(r.success for r in records)} / {len(records)}")
    if table_counter:
        lines.append(f"- top_table_sets: {table_counter.most_common(3)}")
    if projection_counter:
        lines.append(f"- top_projection_patterns: {projection_counter.most_common(3)}")
    if time_pred_counter:
        lines.append(f"- top_time_predicate_patterns: {time_pred_counter.most_common(3)}")
    if value_pred_counter:
        lines.append(f"- top_value_predicate_patterns: {value_pred_counter.most_common(3)}")
    if complexity_counter:
        lines.append(f"- top_complexity_patterns: {complexity_counter.most_common(3)}")
    lines.append(f"- top_cluster_sizes: {[(c['cluster_id'], c['size']) for c in clusters[:6]]}")
    return "\n".join(lines)


def _truncate_text(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "...[TRUNCATED]"


def _compact_join(items: list[str], *, limit: int = 2, empty: str = "<none>") -> str:
    cleaned = [item for item in items if item]
    if not cleaned:
        return empty
    if len(cleaned) <= limit:
        return ", ".join(cleaned)
    return ", ".join(cleaned[:limit]) + f", +{len(cleaned) - limit} more"


def _format_counter_patterns(counter_rows: list[Any], *, limit: int = 2) -> str:
    values: list[str] = []
    for item in counter_rows[:limit]:
        if not item:
            continue
        key = item[0]
        if isinstance(key, (list, tuple)):
            text = "/".join(str(part) for part in key if part)
        else:
            text = str(key)
        if text:
            values.append(text)
    return _compact_join(values, limit=limit)


def _compress_rows_preview(rows_preview: list[list[Any]], *, max_cell_chars: int = 48) -> str:
    if not rows_preview:
        return "<none>"
    first_row = rows_preview[0]
    compact_row = [_truncate_text(str(cell), max_cell_chars) for cell in first_row[:4]]
    if len(first_row) > 4:
        compact_row.append(f"+{len(first_row) - 4} more cols")
    return json.dumps(compact_row, ensure_ascii=False)


def _summarize_predicate_text(text: str, *, max_chars: int) -> str:
    normalized = _normalize_predicate_text(text)
    if not normalized:
        return "<empty>"
    lower = normalized.lower()
    literals = []
    for raw in re.findall(r"'([^']+)'", normalized):
        if raw not in literals:
            literals.append(raw)
        if len(literals) >= 2:
            break
    literal_summary = ""
    if literals:
        literal_summary = " [" + ", ".join(repr(item) for item in literals) + "]"
    if " in (select " in lower:
        lhs, _, _ = normalized.partition(" in (select ")
        return _truncate_text(lhs.strip(), max(24, max_chars - len(literal_summary) - 15)) + " IN <subquery>" + literal_summary
    if " exists (" in lower:
        return "EXISTS <subquery>" + literal_summary
    return _truncate_text(normalized, max_chars)


def _summarize_predicates(predicates: list[str], *, max_items: int = 2, max_chars: int = 96) -> str:
    if not predicates:
        return "<none>"
    parts = [_summarize_predicate_text(item, max_chars=max_chars) for item in predicates[:max_items]]
    if len(predicates) > max_items:
        parts.append(f"+{len(predicates) - max_items} more")
    return "; ".join(parts)


def _sql_snippet(sql: str, *, max_chars: int) -> str:
    return _truncate_text(" ".join((sql or "").split()), max_chars)


def _build_compact_cluster_block(
    cluster: dict[str, Any],
    *,
    cluster_rank: int,
) -> str:
    logic = cluster["logic_summary"]
    table_patterns = _format_counter_patterns(cluster.get("tables_counter") or [])
    projected_columns = _compact_join(
        [str(col) for col in (logic.get("projected_columns") or [])],
        limit=2,
    )
    value_filters = _summarize_predicates(logic.get("value_predicates") or [])
    time_filters = _summarize_predicates(logic.get("time_predicates") or [])
    preview_row = _compress_rows_preview(cluster.get("rows_preview") or [])
    source_summary = _compact_join(
        [str(item[0]) for item in (cluster.get("source_summary") or []) if item],
        limit=2,
    )
    lines = [
        f"{cluster['cluster_id']}:",
        (
            f"- summary: size={cluster['size']} vote_max={cluster['vote_count_max']} "
            f"rep_idx={cluster['representative_candidate_index']} rows={cluster['row_count']} cols={cluster['col_count']}"
        ),
        (
            f"- logic: tables={table_patterns}; select={projected_columns}; "
            f"agg={int(bool(logic['has_aggregate']))} group={int(bool(logic['has_group_by']))} "
            f"window={int(bool(logic['has_window']))} cte={int(bool(logic['has_cte']))} "
            f"complexity={logic['complexity_score']}"
        ),
        f"- time_filters: {time_filters}",
        f"- value_filters: {value_filters}",
        f"- sources: {source_summary}",
        f"- preview_row1: {preview_row}",
    ]
    sql_chars = 520 if cluster_rank < 2 else 0
    if sql_chars > 0:
        lines.append(f"- sql_snippet: {_sql_snippet(cluster['representative_sql'], max_chars=sql_chars)}")
    return "\n".join(lines) + "\n"


def build_cluster_block(
    cluster: dict[str, Any],
    *,
    max_rows_preview_chars: int,
    style: str = "full",
    cluster_rank: int = 0,
) -> str:
    if style == "compact":
        return _build_compact_cluster_block(cluster, cluster_rank=cluster_rank)
    tables_summary = ", ".join(
        "/".join(item[0]) if item and item[0] else "<none>"
        for item in cluster["tables_counter"][:2]
    ) or "<none>"
    projection_summary = ", ".join(
        "/".join(item[0]) if item and item[0] else "<none>"
        for item in cluster["projection_counter"][:2]
    ) or "<none>"
    rows_preview = _truncate_text(
        json.dumps(cluster["rows_preview"], ensure_ascii=False),
        max_rows_preview_chars,
    )
    logic = cluster["logic_summary"]
    return (
        f"{cluster['cluster_id']}:\n"
        f"- cluster_size: {cluster['size']}\n"
        f"- representative_candidate_index: {cluster['representative_candidate_index']}\n"
        f"- max_vote_count: {cluster['vote_count_max']}\n"
        f"- source_summary: {cluster['source_summary']}\n"
        f"- table_patterns: {tables_summary}\n"
        f"- projection_patterns: {projection_summary}\n"
        f"- projected_column_count: {logic['projected_column_count']}\n"
        f"- has_aggregate: {logic['has_aggregate']}\n"
        f"- has_group_by: {logic['has_group_by']}\n"
        f"- has_window: {logic['has_window']}\n"
        f"- has_cte: {logic['has_cte']}\n"
        f"- time_predicates: {logic['time_predicates']}\n"
        f"- value_predicates: {logic['value_predicates']}\n"
        f"- complexity_score: {logic['complexity_score']}\n"
        f"- row_count: {cluster['row_count']}\n"
        f"- col_count: {cluster['col_count']}\n"
        f"- rows_preview: {rows_preview}\n"
        f"- sql:\n{cluster['representative_sql']}\n"
    )


def build_cluster_blocks(
    clusters: list[dict[str, Any]],
    *,
    max_rows_preview_chars_per_cluster: int,
    max_cluster_blocks_chars: int,
    style: str = "full",
) -> str:
    blocks: list[str] = []
    total = 0
    for idx, cluster in enumerate(clusters):
        block = build_cluster_block(
            cluster,
            max_rows_preview_chars=max_rows_preview_chars_per_cluster,
            style=style,
            cluster_rank=idx,
        )
        if max_cluster_blocks_chars > 0 and total + len(block) > max_cluster_blocks_chars:
            if not blocks:
                blocks.append(_truncate_text(block, max_cluster_blocks_chars))
            break
        blocks.append(block)
        total += len(block)
    return "\n".join(blocks)


def summarize_query_logic(query_spec: Any) -> dict[str, Any]:
    projections = getattr(query_spec, "projections", []) or []
    where_predicates = getattr(query_spec, "where_predicates", []) or []
    has_group_by = bool(getattr(query_spec, "group_by", []) or [])
    has_cte = "WITH " in (getattr(query_spec, "sql_text", "") or "").upper()
    sql_text_upper = (getattr(query_spec, "sql_text", "") or "").upper()
    has_window = " OVER " in sql_text_upper
    has_aggregate = any(
        token in sql_text_upper for token in ("COUNT(", "SUM(", "AVG(", "MIN(", "MAX(")
    ) or has_group_by
    projected_columns = [col for proj in projections for col in getattr(proj, "source_columns", [])]
    time_predicates = [
        pred.expression
        for pred in where_predicates
        if any("date" in col.lower() or "time" in col.lower() for col in getattr(pred, "columns", []) or [])
    ]
    value_predicates = [
        pred.expression
        for pred in where_predicates
        if getattr(pred, "literals", None)
    ]
    complexity_score = (
        len(projections)
        + (2 if has_cte else 0)
        + (2 if has_window else 0)
        + (2 if has_aggregate else 0)
        + (1 if has_group_by else 0)
    )
    return {
        "projected_column_count": len(projections),
        "projected_columns": projected_columns,
        "has_aggregate": has_aggregate,
        "has_group_by": has_group_by,
        "has_window": has_window,
        "has_cte": has_cte,
        "time_predicates": time_predicates[:4],
        "value_predicates": value_predicates[:4],
        "normalized_time_predicates": [_normalize_predicate_text(item) for item in time_predicates[:8]],
        "normalized_value_predicates": [_normalize_predicate_text(item) for item in value_predicates[:8]],
        "complexity_score": complexity_score,
    }


def _normalize_predicate_text(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def maybe_apply_minimality_override(chosen_cluster: dict[str, Any] | None, clusters: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str | None]:
    if chosen_cluster is None:
        return None, None
    chosen_logic = chosen_cluster["logic_summary"]
    chosen_tables = tuple(chosen_cluster.get("rep_base_tables") or [])
    chosen_time = tuple(chosen_logic.get("normalized_time_predicates") or [])
    chosen_value = tuple(chosen_logic.get("normalized_value_predicates") or [])

    best = chosen_cluster
    reason = None
    for challenger in clusters:
        if challenger["cluster_id"] == chosen_cluster["cluster_id"]:
            continue
        challenger_logic = challenger["logic_summary"]
        challenger_tables = tuple(challenger.get("rep_base_tables") or [])
        if challenger_tables != chosen_tables:
            continue
        if tuple(challenger_logic.get("normalized_time_predicates") or []) != chosen_time:
            continue
        if tuple(challenger_logic.get("normalized_value_predicates") or []) != chosen_value:
            continue
        if challenger_logic["projected_column_count"] > chosen_logic["projected_column_count"]:
            continue
        complexity_gap = chosen_logic["complexity_score"] - challenger_logic["complexity_score"]
        if complexity_gap < 2:
            continue
        if challenger["size"] < max(1, chosen_cluster["size"] // 4):
            continue
        best = challenger
        reason = (
            f"minimality_override: {chosen_cluster['cluster_id']} -> {challenger['cluster_id']} "
            f"(same tables/predicate skeleton, lower complexity {chosen_logic['complexity_score']} -> {challenger_logic['complexity_score']})"
        )
        break
    return best, reason


def call_qwen3(client: OpenAI, model_name: str, prompt: str, response_max_tokens: int) -> dict[str, Any]:
    rsp = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": "You are a precise SQL candidate judge."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.0,
        top_p=1.0,
        max_tokens=response_max_tokens,
        timeout=60,
    )
    text = rsp.choices[0].message.content or ""
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return {"raw_response": text, "chosen_cluster_id": None}
    try:
        payload = json.loads(match.group(0))
    except Exception:
        payload = {"raw_response": text, "chosen_cluster_id": None}
    payload["raw_response"] = text
    return payload


def execute_sql_signature(db_path: str, sql: str, timeout: int) -> frozenset[tuple[Any, ...]] | None:
    rec = execute_candidate(
        db_path=db_path,
        sql=sql,
        timeout=timeout,
        max_preview_rows=3,
        candidate_index=-1,
        source="gold",
        vote_count=0,
    )
    return rec.rows_signature if rec.success else None


def evaluate_block(
    *,
    block: dict[str, Any],
    run_map: dict[int, dict[str, Any]],
    gold_map: dict[int, dict[str, Any]] | None,
    args: argparse.Namespace,
    model_cfg: dict[str, Any],
) -> dict[str, Any] | None:
    qid = block["question_id"]
    sample = run_map.get(qid)
    gold = gold_map.get(qid) if gold_map is not None else None
    if sample is None:
        return None
    db_id = sample["db_id"]
    db_path = args.db_root / db_id / f"{db_id}.sqlite"

    candidates = sample.get("sql_candidates") or []
    candidates = candidates[: args.max_candidates]
    if args.drop_empty:
        filtered = []
        for cand in candidates:
            execution = cand.get("execution") or {}
            if execution.get("status") == "succeeded" and execution.get("empty_result"):
                continue
            filtered.append(cand)
        candidates = filtered
    exec_records = [
        execute_candidate(
            db_path=str(db_path),
            sql=(cand.get("sql") or "").strip(),
            timeout=args.timeout,
            max_preview_rows=args.max_preview_rows,
            candidate_index=int(cand.get("index", idx)),
            source=str(cand.get("source_model") or cand.get("source") or ""),
            vote_count=int(cand.get("vote_count") or 0),
        )
        for idx, cand in enumerate(candidates)
        if (cand.get("sql") or "").strip()
    ]
    exec_records = [record for record in exec_records if record.success]
    if not exec_records:
        return None
    if args.group_mode == "hypothesis":
        clusters = group_hypotheses(exec_records)
    else:
        clusters = cluster_candidates(exec_records)
    if not clusters:
        return None
    clusters = clusters[: args.max_clusters]

    schema_text = sample.get("schema_info", {}).get("schema_description") or sample.get("schema_info", {}).get("schema_prompt_text") or ""
    if len(schema_text) > args.max_schema_chars:
        schema_text = schema_text[: args.max_schema_chars] + "\n...[TRUNCATED]"

    task_summary = json.dumps(
        summarize_task(sample.get("question", ""), sample.get("evidence", "")),
        ensure_ascii=False,
        indent=2,
    )
    evidence_synthesis = synthesize_evidence(
        sample.get("question", ""),
        sample.get("evidence", ""),
        exec_records,
        clusters,
    )
    cluster_blocks = build_cluster_blocks(
        clusters,
        max_rows_preview_chars_per_cluster=args.max_rows_preview_chars_per_cluster,
        max_cluster_blocks_chars=args.max_cluster_blocks_chars,
        style=args.cluster_prompt_style,
    )
    prompt = CHOICE_PROMPT.format(
        question=sample.get("question", ""),
        evidence=sample.get("evidence", "") or "<empty>",
        schema_text=schema_text or "<empty>",
        task_summary=task_summary,
        evidence_synthesis=evidence_synthesis,
        cluster_blocks=cluster_blocks,
    )
    prompt_stats = {
        "prompt_chars": len(prompt),
        "schema_chars": len(schema_text),
        "task_summary_chars": len(task_summary),
        "evidence_synthesis_chars": len(evidence_synthesis),
        "cluster_blocks_chars": len(cluster_blocks),
        "cluster_count": len(clusters),
        "successful_candidates": len(exec_records),
        "cluster_prompt_style": args.cluster_prompt_style,
    }

    llm_choice: dict[str, Any] = {}
    chosen_cluster_id = None
    chosen_cluster = None
    override_reason = None
    chosen_sql = None
    llm_error = None
    if not args.dry_run_prompts:
        try:
            client = OpenAI(base_url=model_cfg["base_url"], api_key=model_cfg["api_key"])
            llm_choice = call_qwen3(client, model_cfg["model_name"], prompt, args.response_max_tokens)
            chosen_cluster_id = llm_choice.get("chosen_cluster_id")
            chosen_cluster = next((cluster for cluster in clusters if cluster["cluster_id"] == chosen_cluster_id), None)
            overridden_cluster, override_reason = maybe_apply_minimality_override(chosen_cluster, clusters)
            if overridden_cluster is not None:
                chosen_cluster = overridden_cluster
                chosen_cluster_id = chosen_cluster["cluster_id"]
            chosen_sql = chosen_cluster["representative_sql"] if chosen_cluster else None
        except Exception as exc:  # pragma: no cover - online model failures
            llm_error = f"{type(exc).__name__}: {exc}"
    result = {
        "question_id": qid,
        "db_id": db_id,
        "major_sql": block["major_sql"],
        "pass_sql": block["pass_sql"],
        "chosen_cluster_id": chosen_cluster_id,
        "chosen_sql": chosen_sql,
        "chosen_cluster_size": chosen_cluster["size"] if chosen_cluster else None,
        "override_reason": override_reason,
        "prompt_stats": prompt_stats,
        "clusters": [
            {
                "cluster_id": cluster["cluster_id"],
                "size": cluster["size"],
                "representative_candidate_index": cluster["representative_candidate_index"],
                "representative_sql": cluster["representative_sql"],
                "row_count": cluster["row_count"],
                "logic_summary": cluster["logic_summary"],
            }
            for cluster in clusters
        ],
        "llm_response": llm_choice.get("raw_response"),
        "llm_error": llm_error,
    }
    if args.dry_run_prompts:
        result["is_correct"] = None
    elif gold is not None:
        gold_sql = gold["SQL"]
        gold_sig = execute_sql_signature(str(db_path), gold_sql, args.timeout)
        chosen_sig = execute_sql_signature(str(db_path), chosen_sql, args.timeout) if chosen_sql else None
        result["is_correct"] = bool(chosen_sig is not None and gold_sig is not None and chosen_sig == gold_sig)
    else:
        result["is_correct"] = None
    return result


def main() -> None:
    args = parse_args()
    run_payload = json.loads(args.run.read_text(encoding="utf-8"))
    run_map = {int(row["question_id"]): row for row in run_payload.get("results", [])}
    gold_map = None
    if args.gold:
        gold_rows = json.loads(args.gold.read_text(encoding="utf-8"))
        gold_map = {int(row["question_id"]): row for row in gold_rows}

    if args.qid_list:
        qids = [
            int(line.strip())
            for line in args.qid_list.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        blocks = [
            {
                "question_id": qid,
                "db_id": run_map.get(qid, {}).get("db_id", ""),
                "pass_position": None,
                "candidate_index": None,
                "major_sql": "",
                "pass_sql": "",
            }
            for qid in qids
            if qid in run_map
        ]
    else:
        if not args.pass_report:
            raise ValueError("--pass-report is required unless --qid-list is provided")
        pass_blocks = parse_major_miss_blocks(args.pass_report.read_text(encoding="utf-8"))
        blocks = pass_blocks
    blocks = blocks[: args.max_samples]

    model_cfg = get_model_config(args.model_key)
    results = []
    max_workers = max(1, args.max_workers)
    if max_workers == 1:
        total_blocks = len(blocks)
        for idx, block in enumerate(blocks, start=1):
            item = evaluate_block(
                block=block,
                run_map=run_map,
                gold_map=gold_map,
                args=args,
                model_cfg=model_cfg,
            )
            if item is not None:
                results.append(item)
            if idx % 10 == 0 or idx == total_blocks:
                logger.info(
                    "QwenSelector: progress {}/{} (results={})",
                    idx,
                    total_blocks,
                    len(results),
                )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    evaluate_block,
                    block=block,
                    run_map=run_map,
                    gold_map=gold_map,
                    args=args,
                    model_cfg=model_cfg,
                )
                for block in blocks
            ]
            total_futures = len(futures)
            completed = 0
            for future in as_completed(futures):
                item = future.result()
                completed += 1
                if item is not None:
                    results.append(item)
                if completed % 10 == 0 or completed == total_futures:
                    logger.info(
                        "QwenSelector: progress {}/{} (results={})",
                        completed,
                        total_futures,
                        len(results),
                    )

    results.sort(key=lambda item: item["question_id"])
    total = len(results)
    correct = sum(int(bool(item["is_correct"])) for item in results if item.get("is_correct") is not None)

    summary = {
        "evaluated": total,
        "model_key": args.model_key,
        "max_samples": args.max_samples,
        "max_clusters": args.max_clusters,
        "group_mode": args.group_mode,
        "cluster_prompt_style": args.cluster_prompt_style,
        "max_workers": max_workers,
        "drop_empty": bool(args.drop_empty),
        "dry_run_prompts": bool(args.dry_run_prompts),
    }
    if gold_map is not None and not args.dry_run_prompts:
        summary["correct"] = correct
        summary["accuracy"] = (correct / total) if total else 0.0
    output = {"summary": summary, "results": results}
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
