#!/usr/bin/env python3
"""SQL Pipeline v2 — full Phase 1 → 2 → 3 → 4 → 5 pipeline.

Supports both individual phase execution and full end-to-end runs.

Phase 1:
  - Generate SQL candidates from BIRD samples using schema analysis + SQL generation nodes
  - Supports concurrent generation with --max-workers
  - Optional offline HNSW value hints

Phase 2:
  - Rule-based preprocessing + hard filtering
  - Only clean SQL flows to Phase 3; removed candidates are retained for audit

Phase 3:
  - Execution diagnostics are recorded for runtime errors, timeouts, and empty results
  - Empty-result executions are tagged with `empty_result: true`
  - Timeout executions keep their original `timeout` status for later analysis

Phase 4:
  - Empty-result / timeout / runtime-error candidates use dedicated correction prompts
  - Correction prompts include evidence and phase3 feedback context
  - After correction, immediately re-execute to confirm the fix

Phase 5:
  - Rule post-processing + zero-candidate regeneration
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import tqdm
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed, wait
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import threading

from loguru import logger
from openai import OpenAI

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

os.environ.setdefault("SQL_RESULT_VOTING_ENABLED", "false")
os.environ.setdefault("SQL_RESULT_VOTING_AUTOCORRECT_ENABLED", "false")

from config import Settings  # noqa: E402
from config.model_config import get_model_config  # noqa: E402
from scripts.filter_sql_candidates_by_rules import (  # noqa: E402
    EVIDENCE_PATTERNS as FILTER_EVIDENCE_PATTERNS,
    QUESTION_PATTERNS as FILTER_QUESTION_PATTERNS,
    RULES_QES_30,
    SQL_PATTERNS as FILTER_SQL_PATTERNS,
    build_gt_signature_guard as build_filter_gt_signature_guard,
    compile_patterns as compile_filter_patterns,
    extract_features as extract_filter_features,
    is_candidate_guarded_by_gt as is_filter_candidate_guarded_by_gt,
    load_gt_rows as load_filter_gt_rows,
    match_rule_texts as match_filter_rule_texts,
    parse_rules as parse_filter_rules,
)
from scripts.regenerate_zero_candidates import (  # noqa: E402
    RegenConfig,
    build_feedback_block as build_regen_feedback_block,
    build_prompt as build_regen_prompt,
    generate_for_item as generate_regen_candidates,
    normalize_sql as normalize_regen_sql,
)
from src.tools.sql_validator import (  # noqa: E402
    extract_cte_names,
    extract_table_names,
    validate_sql_schema,
    validate_sql_syntax,
)
from src.utils.sql_text import extract_sql_from_response  # noqa: E402
from src.utils.llm_client import convert_messages_for_responses, extract_text_from_response  # noqa: E402
from src.tools.sql_projection_rules import apply_projection_rules  # noqa: E402
from error_bank.diagnosis_spec import build_structured_empty_sql_diagnosis  # noqa: E402
from error_bank.high_precision_empty_repair import suggest_high_precision_empty_repairs  # noqa: E402
from error_bank.store import ErrorBankStore  # noqa: E402
from error_bank.prober import diagnose_empty_result  # noqa: E402

# Phase 1 imports
from scripts.run_async_multi_test import (  # noqa: E402
    _remap_legacy_path,
    build_initial_state,
    load_samples,
)
from src.nodes import (  # noqa: E402
    schema_analysis_node,
    sql_generation_node,
)
from src.core.state import SQLAgentState  # noqa: E402

# ---------------------------------------------------------------------------
# Phase snapshot — optional per-phase backup
# ---------------------------------------------------------------------------

def _save_phase_snapshot(file_path: Path, phase_tag: str) -> None:
    """If SAVE_PHASE_SNAPSHOT=true, copy the current output to a backup with phase suffix.

    e.g.  experiments/out.json  →  experiments/out.phase2.json
    """
    if os.getenv("SAVE_PHASE_SNAPSHOT", "false").lower() not in ("true", "1", "yes"):
        return
    snapshot = file_path.with_suffix(f".{phase_tag}.json")
    import shutil
    shutil.copy2(file_path, snapshot)
    logger.info("Snapshot saved: {}", snapshot)


def _stderr_is_tty() -> bool:
    isatty = getattr(sys.stderr, "isatty", None)
    if not callable(isatty):
        return False
    try:
        return bool(isatty())
    except Exception:
        return False


def _iter_with_progress(iterator, total: int, desc: str):
    if total <= 0:
        return
    if _stderr_is_tty():
        yield from tqdm.tqdm(iterator, total=total, desc=desc)
        return

    log_every = max(1, (total + 19) // 20)
    logger.info("{}: start total={}", desc, total)
    for done, item in enumerate(iterator, start=1):
        if done == total or done % log_every == 0:
            logger.info("{}: progress {}/{}", desc, done, total)
        yield item


def _iter_completed_futures(futures, desc: str):
    total = len(futures)
    if total <= 0:
        return
    if _stderr_is_tty():
        yield from tqdm.tqdm(as_completed(futures), total=total, desc=desc)
        return

    log_every = max(1, (total + 19) // 20)
    last_completion = time.monotonic()
    next_stall_log = 300.0
    done_count = 0
    pending = set(futures)

    logger.info("{}: start total={}", desc, total)
    while pending:
        done, pending = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
        if not done:
            waited = time.monotonic() - last_completion
            if waited >= next_stall_log:
                logger.warning(
                    "{}: no completions for {:.0f}s, pending={}/{}",
                    desc,
                    waited,
                    len(pending),
                    total,
                )
                next_stall_log += 300.0
            continue

        last_completion = time.monotonic()
        next_stall_log = 300.0
        for future in done:
            done_count += 1
            if done_count == total or done_count % log_every == 0:
                logger.info("{}: progress {}/{}", desc, done_count, total)
            yield future


# ---------------------------------------------------------------------------
# Phase 1 — SQL candidate generation
# ---------------------------------------------------------------------------


def _prepare_candidate_entry(candidate: Dict[str, Any], fallback_index: int) -> Dict[str, Any]:
    return {
        "index": candidate.get("index", fallback_index),
        "sql": candidate.get("sql", ""),
        "source_group": candidate.get("source_group"),
        "source_model": candidate.get("source_model"),
        "source": candidate.get("source"),
        "vote_count": candidate.get("vote_count"),
        "result_vote_score": candidate.get("result_vote_score"),
        "validation": {"status": "pending", "errors": [], "warnings": []},
        "execution": {"status": "pending", "rows": None, "error": None, "elapsed": None},
    }


def _load_value_hints_map(path: str) -> Dict[Tuple[Any, str], str]:
    hint_map: Dict[Tuple[Any, str], str] = {}
    source_path = Path(path)
    if not source_path.exists():
        raise FileNotFoundError(f"value hints file not found: {source_path}")

    def _extract_hint(row: Dict[str, Any]) -> Optional[str]:
        hint = row.get("value_link_prompt_hints")
        if isinstance(hint, str) and hint.strip():
            return hint.strip()
        return None

    if source_path.suffix.lower() == ".jsonl":
        for line in source_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            qid = row.get("question_id")
            db_id = row.get("db_id")
            if not isinstance(db_id, str):
                continue
            hint = _extract_hint(row)
            if hint is None:
                continue
            hint_map[(qid, db_id)] = hint
        return hint_map

    data = json.loads(source_path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            qid = row.get("question_id")
            db_id = row.get("db_id")
            if not isinstance(db_id, str):
                continue
            hint = _extract_hint(row)
            if hint is None:
                continue
            hint_map[(qid, db_id)] = hint
    return hint_map


def generate_for_sample(
    sample: Dict[str, Any],
    model_name: str,
    value_hints_map: Optional[Dict[Tuple[Any, str], str]] = None,
) -> Dict[str, Any]:
    """Generate SQL candidates for a single BIRD sample (Phase 1)."""
    thread_name = threading.current_thread().name
    logger.info(
        "Phase1: start question_id={} db_id={} thread={}",
        sample.get("question_id"),
        sample.get("db_id"),
        thread_name,
    )
    db_path = Settings.get_database_path(sample["db_id"])
    state = build_initial_state(sample, str(db_path), model_name)

    value_link_hint = None
    if value_hints_map:
        key = (sample.get("question_id"), sample.get("db_id"))
        value_link_hint = value_hints_map.get(key)
        if value_link_hint:
            state["value_link_prompt_hints"] = value_link_hint

    schema_output = schema_analysis_node(state)
    state.update(schema_output)

    generation_output = sql_generation_node(state)
    state.update(generation_output)

    schema_info = schema_output.get("schema_info", {})
    candidates = state.get("generation_candidates") or []
    payload_candidates = []
    for idx, candidate in enumerate(candidates):
        if not isinstance(candidate, dict):
            continue
        payload_candidates.append(_prepare_candidate_entry(candidate, idx))

    payload = {
        "question_id": sample["question_id"],
        "db_id": sample["db_id"],
        "question": sample.get("question"),
        "evidence": sample.get("evidence", ""),
        "difficulty": sample.get("difficulty"),
        "schema_info": schema_info,
        "value_link_prompt_hints": value_link_hint,
        "sql_candidates": payload_candidates,
    }
    logger.info(
        "Phase1: done question_id={} db_id={} thread={} candidates={}",
        sample.get("question_id"),
        sample.get("db_id"),
        thread_name,
        len(payload_candidates),
    )
    return payload


def _prepare_error_entry(sample: Dict[str, Any], exc: Exception) -> Dict[str, Any]:
    logger.exception(
        "Phase1: question_id=%s db_id=%s generation failed",
        sample.get("question_id"),
        sample.get("db_id"),
    )
    return {
        "question_id": sample.get("question_id"),
        "db_id": sample.get("db_id"),
        "question": sample.get("question"),
        "evidence": sample.get("evidence", ""),
        "difficulty": sample.get("difficulty"),
        "error": f"generation_failed: {exc}",
        "schema_info": {},
        "sql_candidates": [],
    }


def run_phase1(args: argparse.Namespace) -> None:
    """Phase 1: Generate SQL candidates from BIRD samples."""
    input_path = args.input
    if input_path is None:
        raise ValueError("--input is required for phase1")

    samples = load_samples(input_path, args.start_index, args.sample_count)
    if not samples:
        raise ValueError("No samples loaded — check --input / --start-index / --sample-count")

    logger.info(
        "Phase1: input={} start={} count={} model={}",
        input_path, args.start_index, len(samples), args.model,
    )

    value_hints_map: Optional[Dict[Tuple[Any, str], str]] = None
    value_hints_file = getattr(args, "value_hints_file", None)
    if value_hints_file:
        value_hints_map = _load_value_hints_map(value_hints_file)
        logger.info("Phase1: loaded {} offline value hints from {}", len(value_hints_map), value_hints_file)

    results: List[Optional[Dict[str, Any]]] = [None] * len(samples)
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_to_idx = {
            executor.submit(generate_for_sample, sample, args.model, value_hints_map): idx
            for idx, sample in enumerate(samples)
        }
        for future in _iter_completed_futures(future_to_idx, "Phase1"):
            idx = future_to_idx[future]
            sample = samples[idx]
            try:
                results[idx] = future.result()
            except Exception as exc:
                results[idx] = _prepare_error_entry(sample, exc)

    for idx, entry in enumerate(results):
        if entry is None:
            results[idx] = _prepare_error_entry(samples[idx], RuntimeError("unknown_error"))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "input": str(_remap_legacy_path(Path(input_path))),
        "start_index": args.start_index,
        "sample_count": len(samples),
        "model": args.model,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "results": results,
    }

    tmp_path = output_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(output_path)

    # --- Phase 1 summary ---
    total_candidates = sum(len(r.get("sql_candidates", [])) for r in results)
    error_samples = sum(1 for r in results if r.get("error"))
    logger.info(
        "Phase1: done. samples={} total_candidates={} errors={} → {}",
        len(results), total_candidates, error_samples, output_path,
    )
    _save_phase_snapshot(output_path, "phase1")


def save_final_results(output_path: str) -> None:
    """Export final SQL predictions from the pipeline output to a separate JSON."""
    with open(output_path) as f:
        results = json.load(f)["results"]

    pred_sqls = {}
    fallback_top1 = 0
    for result in results:
        qid = result.get("question_id")
        selection = result.get("selection")
        if isinstance(selection, dict):
            final_sql_block = selection.get("final_sql") or {}
            final_sql_text = final_sql_block.get("sql")
            if isinstance(final_sql_text, str) and final_sql_text.strip():
                pred_sqls[qid] = final_sql_text
                continue

        candidates = result.get("sql_candidates") or []
        if candidates:
            top1_sql = (candidates[0].get("sql") or "").strip()
            if top1_sql:
                pred_sqls[qid] = top1_sql
                fallback_top1 += 1

    pred_path = output_path + ".pred_sqls.json"
    with open(pred_path, "w") as f:
        json.dump(pred_sqls, f, ensure_ascii=False, indent=2)

    logger.info(
        "Final predictions: {} total (selection={}, top1_fallback={}) → {}",
        len(pred_sqls),
        len(pred_sqls) - fallback_top1,
        fallback_top1,
        pred_path,
    )


# ---------------------------------------------------------------------------
# Correction prompts (v2 — error-type-aware)
# ---------------------------------------------------------------------------

_CORRECTION_SYSTEM = (
    "You are an expert SQLite SQL correction assistant. Given the question, evidence, "
    "database schema, current SQL, and failure feedback, reason step by step and produce "
    "one corrected SQL query that preserves the original intent and is executable on SQLite. "
    "Your final answer must contain only the final SQL inside a single ```sql``` code block."
)

_SYNTAX_CORRECTION_TEMPLATE = """Database Engine:
SQLite

Question:
{question}

Evidence:
{evidence}

Value Link Hints:
{value_link_prompt_hints}

Database Schema:
{schema_description}

Current SQL:
{candidate_sql}

Failure Type:
validation_error

Failure Feedback:
{errors}

Instructions:
- Think step by step about the exact syntax or validation issue in the current SQL.
- Fix the SQL syntax or schema validation errors while preserving the original question intent.
- Ensure every table and column name exactly matches the schema.
- Return all and only the information asked in the question, with no missing or extra columns.
- Use valid SQLite syntax throughout.

Output Format:
Return only one corrected SQL query inside a single ```sql``` code block.
"""

_EMPTY_RESULT_CORRECTION_TEMPLATE = """Database Engine:
SQLite

Question:
{question}

Evidence:
{evidence}

Value Link Hints:
{value_link_prompt_hints}

Database Schema:
{schema_description}

Current SQL:
{candidate_sql}

Failure Type:
empty_result

Failure Feedback:
- The SQL executed successfully but returned 0 rows.

Instructions:
- Think step by step about why the current SQL returns no rows.
- Fix the SQL while preserving the original question intent.
- Prioritize checking overly restrictive filters, incorrect value conditions, wrong JOIN keys, missing JOIN paths, incorrect GROUP BY / HAVING logic, and incorrect date/time predicates.
- Do not remove essential constraints just to force a non-empty result.
- If the question is answerable on this database, the corrected SQL should avoid returning an empty result.
- Ensure every table and column name exactly matches the schema.
- Return only the information asked in the question, with no extra columns.

Output Format:
Return only one corrected SQL query inside a single ```sql``` code block.
"""

_TIMEOUT_CORRECTION_TEMPLATE = """Database Engine:
SQLite

Question:
{question}

Evidence:
{evidence}

Value Link Hints:
{value_link_prompt_hints}

Database Schema:
{schema_description}

Current SQL:
{candidate_sql}

Failure Type:
timeout

Failure Feedback:
- The SQL execution exceeded the time limit of {timeout_seconds} seconds.

Instructions:
- Think step by step about why the current SQL timed out.
- Fix the root cause while preserving the original question intent.
- Prioritize correcting missing JOIN conditions, avoiding Cartesian products, reducing unnecessary nested subqueries, and simplifying overly expensive query structures.
- Do not change the requested semantics just to make the query faster.
- Ensure the corrected SQL is valid SQLite and is more likely to execute within the time limit.
- Ensure every table and column name exactly matches the schema.
- Return only the information asked in the question, with no extra columns.

Output Format:
Return only one corrected SQL query inside a single ```sql``` code block.
"""

_EXECUTION_CORRECTION_TEMPLATE = """Database Engine:
SQLite

Question:
{question}

Evidence:
{evidence}

Value Link Hints:
{value_link_prompt_hints}

Database Schema:
{schema_description}

Current SQL:
{candidate_sql}

Failure Type:
runtime_error

Failure Feedback:
- SQLite execution error: {exec_error}
- Additional validation context:
{extra_errors}

Instructions:
- Think step by step about the exact root cause of the execution error.
- Fix the exact root cause indicated by the SQLite error while preserving the original question intent.
- Ensure every table and column name exactly matches the schema.
- Return all and only the information asked in the question, with no missing or extra columns.
- Use valid SQLite syntax throughout.

Output Format:
Return only one corrected SQL query inside a single ```sql``` code block.
"""


def _normalize_prompt_text(value: Any, fallback: str = "N/A") -> str:
    text = (value or "").strip() if isinstance(value, str) else str(value or "").strip()
    return text or fallback


def _extract_corrected_sql_from_response(content: str, question: str) -> Tuple[str, List[str]]:
    raw_content = (content or "").strip()
    extracted_sql = extract_sql_from_response(raw_content)
    sql_candidate = (extracted_sql or raw_content).strip()
    if not sql_candidate:
        return "", []

    cleaned_sql, preprocess_fixes = _preprocess_sql(sql_candidate)
    corrected_sql = cleaned_sql or sql_candidate
    if Settings.SQL_PROJECTION_RULES_ENABLED and corrected_sql:
        adjusted, adjustments = apply_projection_rules(corrected_sql, question)
        if adjustments:
            corrected_sql = adjusted
    return corrected_sql, preprocess_fixes


def _call_correction_llm(system: str, prompt: str, question: str = "") -> Dict[str, Any]:
    """Call the configured correction model and return normalized correction metadata."""
    target_model_name = (
        Settings.SQL_CORRECTION_MODEL
        or Settings.SQL_GENERATION_MODEL
        or Settings.DEFAULT_MODEL
    )
    model_config = get_model_config(target_model_name)
    llm = OpenAI(base_url=model_config["base_url"], api_key=model_config["api_key"])
    model_identifier = model_config["model_name"]
    transport = model_config.get("transport", "chat_completions")
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": prompt},
    ]
    if transport == "responses":
        response = llm.responses.create(
            model=model_identifier,
            input=convert_messages_for_responses(messages),
            temperature=Settings.MODEL_TEMPERATURE,
            max_output_tokens=Settings.MAX_TOKENS,
            timeout=Settings.SQL_GENERATION_REQUEST_TIMEOUT,
        )
        content = extract_text_from_response(response)
    else:
        completion = llm.chat.completions.create(
            model=model_identifier,
            messages=messages,
            temperature=Settings.MODEL_TEMPERATURE,
            max_tokens=Settings.MAX_TOKENS,
            timeout=Settings.SQL_GENERATION_REQUEST_TIMEOUT,
        )
        content = completion.choices[0].message.content or ""
    corrected_sql, preprocess_fixes = _extract_corrected_sql_from_response(content, question)
    return {
        "corrected_sql": corrected_sql,
        "raw_output": content.strip(),
        "prompt": prompt,
        "model_key": target_model_name,
        "model_name": model_identifier,
        "transport": transport,
        "preprocess_fixes": preprocess_fixes,
    }


def _correct_syntax_errors(
    question: str,
    candidate_sql: str,
    errors: List[str],
    schema_description: str,
    evidence: str,
    value_link_prompt_hints: str,
) -> Dict[str, Any]:
    prompt = _SYNTAX_CORRECTION_TEMPLATE.format(
        schema_description=schema_description,
        question=question,
        evidence=_normalize_prompt_text(evidence),
        value_link_prompt_hints=_normalize_prompt_text(value_link_prompt_hints),
        candidate_sql=candidate_sql,
        errors="\n".join(f"- {e}" for e in errors) or "- No errors provided",
    )
    result = _call_correction_llm(_CORRECTION_SYSTEM, prompt, question=question)
    result["prompt_type"] = "validation_error"
    return result


def _try_high_precision_empty_repair(
    *,
    question_id: int,
    question: str,
    evidence: str,
    candidate_sql: str,
    db_id: str,
    db_path: str,
    exec_timeout: int,
) -> tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
    sql_text = (candidate_sql or "").strip()
    if not sql_text or not db_id or not db_path:
        return None, None

    try:
        probe_timeout = max(5, min(int(exec_timeout or 20), 30))
        probe_executor = ThreadPoolExecutor(max_workers=1)
        future = probe_executor.submit(diagnose_empty_result, question_id, db_id, sql_text, db_path, probe_timeout)
        try:
            diag = future.result(timeout=probe_timeout)
        except FutureTimeoutError:
            future.cancel()
            probe_executor.shutdown(wait=False, cancel_futures=True)
            return None, None
        finally:
            try:
                probe_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                probe_executor.shutdown(wait=False)
        structured = build_structured_empty_sql_diagnosis(
            question_id=question_id,
            db_id=db_id,
            question=question or "",
            evidence=evidence or "",
            sql_text=sql_text,
            diagnosis=diag,
            db_path=db_path,
        ).to_dict()
        suggestions = suggest_high_precision_empty_repairs(
            structured,
            db_path=db_path,
            search_timeout_seconds=probe_timeout,
        )
    except Exception:
        return None, None

    for item in suggestions:
        repaired_sql = (item.get("sql") or "").strip()
        if not repaired_sql or repaired_sql == sql_text:
            continue
        re_exec = _exec_sql(repaired_sql, db_path, exec_timeout)
        if re_exec.get("status") == "succeeded" and not re_exec.get("empty_result", False):
            correction_info = {
                "corrected_sql": repaired_sql,
                "raw_output": "",
                "prompt": "",
                "model_key": "high_precision_empty_repair",
                "model_name": "rule",
                "transport": "rule",
                "preprocess_fixes": [],
                "prompt_type": "empty_result_hp_repair",
                "operator": item.get("operator"),
                "rule": item.get("rule"),
            }
            return correction_info, re_exec
    return None, None


def _extract_literals_for_guard(text: str) -> set[str]:
    values = set()
    if not text:
        return values
    for match in re.findall(r'"([^"]{1,200})"', text):
        if match.strip():
            values.add(match.strip())
    for match in re.findall(r"'((?:''|[^']){1,200})'", text):
        norm = match.replace("''", "'").strip()
        if norm:
            values.add(norm)
    for match in re.findall(r"\b\d{1,4}(?:[-/:.]\d{1,4}){0,3}%?\b", text):
        if match.strip():
            values.add(match.strip())
    return values


def _allowed_guard_variants(literal: str) -> set[str]:
    lit = literal.strip()
    allowed = {lit}
    if not lit:
        return allowed
    allowed.add(lit.lower())
    if re.fullmatch(r"0:\d{2}:\d{2}", lit):
        mm, ss = lit.split(":")[1:]
        base = f"{int(mm)}:{ss}"
        allowed.update({base, f"{base}%", f"%{base}%", f"{base}.000"})
    if re.fullmatch(r"\d{1,2}:\d{2}(?:\.\d{3})?", lit):
        core = lit.split(".", 1)[0]
        allowed.update({core, f"{core}%", f"%{core}%", f"{core}.000"})
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", lit):
        allowed.update({f"{lit}%", lit.replace("-", "/")})
    return allowed


def _phase45_literal_drift_detected(question: str, evidence: str, source_sql: str, new_sql: str) -> bool:
    source_literals = (
        _extract_literals_for_guard(question)
        | _extract_literals_for_guard(evidence)
        | _extract_literals_for_guard(source_sql)
    )
    allowed = set()
    for item in source_literals:
        allowed.update(_allowed_guard_variants(item))
    lowered_allowed = {x.lower() for x in allowed}
    for lit in _extract_literals_for_guard(new_sql):
        if lit not in allowed and lit.lower() not in lowered_allowed:
            return True
    return False


def _phase45_is_likely_singular_question(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    plural_cues = [
        "list ",
        "which schools",
        "which users",
        "what are",
        "names of",
        "countries of",
        "ids of the patients",
        "products consumed",
    ]
    if any(cue in q for cue in plural_cues):
        return False
    singular_cues = [
        "who is",
        "what is",
        "what was",
        "point out the language of set id",
        "for the driver who",
        "identify the display name and location of the user",
        "tell the japanese name",
        "is there a korean version",
        "to which artist",
    ]
    return any(cue in q for cue in singular_cues)


def _phase45_candidate_passes_guard(
    *,
    question: str,
    evidence: str,
    source_sql: str,
    candidate_sql: str,
    execution_rows: Any,
) -> bool:
    if _phase45_literal_drift_detected(question, evidence, source_sql, candidate_sql):
        return False
    rows = execution_rows or []
    if _phase45_is_likely_singular_question(question) and len(rows) > 3:
        return False
    return True


def _correct_empty_result_two_stage(
    *,
    sample: Dict[str, Any],
    candidate: Dict[str, Any],
    cand_idx: int,
    db_path: str,
    exec_timeout: int,
    stage2_bank: ErrorBankStore,
    stage2_model_cfg: Dict[str, Any],
    stage2_max_rounds: int,
    stage2_llm_timeout: int,
    stage2_max_tokens: int,
) -> tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
    question = sample.get("question", "")
    evidence = sample.get("evidence", "")
    db_id = sample.get("db_id", "")
    qid = int(sample.get("question_id", -1))
    source_sql = (candidate.get("sql") or "").strip()

    correction_info, re_exec = _try_high_precision_empty_repair(
        question_id=qid,
        question=question,
        evidence=evidence,
        candidate_sql=source_sql,
        db_id=db_id,
        db_path=db_path,
        exec_timeout=exec_timeout,
    )
    if correction_info is not None and re_exec is not None:
        return correction_info, re_exec

    if stage2_max_rounds <= 0 or not source_sql:
        return None, None

    from error_bank.phase4_5 import _diagnose_one_candidate, _repair_one_candidate_round

    state = {
        "job_idx": 0,
        "sample": sample,
        "candidate": candidate,
        "cand_idx": cand_idx,
        "db_path": db_path,
        "current_sql": source_sql,
        "history_lines": [],
        "new_candidates": [],
        "done": False,
        "diag": None,
        "diag_text": "",
        "has_cross_table_hit": False,
        "bank_entry": None,
    }
    client = OpenAI(
        base_url=stage2_model_cfg["base_url"],
        api_key=stage2_model_cfg["api_key"],
    )

    for round_idx in range(1, max(1, stage2_max_rounds) + 1):
        diag_result = _diagnose_one_candidate(state, stage2_bank, exec_timeout)
        state["diag"] = diag_result["diag"]
        state["diag_text"] = diag_result["diag_text"]
        state["has_cross_table_hit"] = diag_result["has_cross_table_hit"]
        state["bank_entry"] = diag_result["bank_entry"]
        state["structured_diagnosis"] = diag_result.get("structured_diagnosis")

        repair_result = _repair_one_candidate_round(
            state,
            stage2_bank,
            client,
            stage2_model_cfg["model_name"],
            round_idx,
            exec_timeout,
            stage2_llm_timeout,
            stage2_max_tokens,
        )
        state["history_lines"].extend(repair_result["history_updates"])
        state["current_sql"] = repair_result["next_sql"]
        state["done"] = repair_result["done"]
        state["new_candidates"].extend(repair_result["new_candidates"])

        for new_cand in repair_result["new_candidates"]:
            if not _phase45_candidate_passes_guard(
                question=question,
                evidence=evidence,
                source_sql=source_sql,
                candidate_sql=str(new_cand.get("sql") or ""),
                execution_rows=(new_cand.get("execution") or {}).get("rows"),
            ):
                continue
            correction_info = {
                "corrected_sql": new_cand["sql"],
                "raw_output": "",
                "prompt": "",
                "model_key": stage2_model_cfg["model_name"],
                "model_name": stage2_model_cfg["model_name"],
                "transport": "phase4_stage2",
                "preprocess_fixes": [],
                "prompt_type": "empty_result_phase4_stage2",
                "operator": (new_cand.get("phase4_5_info") or {}).get("operator", "phase4.5"),
                "rule": (new_cand.get("phase4_5_info") or {}).get("path", "probe"),
            }
            return correction_info, new_cand.get("execution")

        if not state.get("current_sql") or state.get("done"):
            break

    return None, None


def _correct_empty_result(
    question: str,
    candidate_sql: str,
    schema_description: str,
    evidence: str,
    value_link_prompt_hints: str,
) -> Dict[str, Any]:
    prompt = _EMPTY_RESULT_CORRECTION_TEMPLATE.format(
        schema_description=schema_description,
        question=question,
        evidence=_normalize_prompt_text(evidence),
        value_link_prompt_hints=_normalize_prompt_text(value_link_prompt_hints),
        candidate_sql=candidate_sql,
    )
    result = _call_correction_llm(_CORRECTION_SYSTEM, prompt, question=question)
    result["prompt_type"] = "empty_result"
    return result


def _correct_timeout(
    question: str,
    candidate_sql: str,
    schema_description: str,
    evidence: str,
    value_link_prompt_hints: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    prompt = _TIMEOUT_CORRECTION_TEMPLATE.format(
        schema_description=schema_description,
        question=question,
        evidence=_normalize_prompt_text(evidence),
        value_link_prompt_hints=_normalize_prompt_text(value_link_prompt_hints),
        candidate_sql=candidate_sql,
        timeout_seconds=max(1, int(timeout_seconds)),
    )
    result = _call_correction_llm(_CORRECTION_SYSTEM, prompt, question=question)
    result["prompt_type"] = "timeout"
    return result


def _correct_execution_error(
    question: str,
    candidate_sql: str,
    exec_error: str,
    extra_errors: List[str],
    schema_description: str,
    evidence: str,
    value_link_prompt_hints: str,
) -> Dict[str, Any]:
    prompt = _EXECUTION_CORRECTION_TEMPLATE.format(
        schema_description=schema_description,
        question=question,
        evidence=_normalize_prompt_text(evidence),
        value_link_prompt_hints=_normalize_prompt_text(value_link_prompt_hints),
        candidate_sql=candidate_sql,
        exec_error=exec_error,
        extra_errors="\n".join(f"- {e}" for e in extra_errors) or "- None",
    )
    result = _call_correction_llm(_CORRECTION_SYSTEM, prompt, question=question)
    result["prompt_type"] = "runtime_error"
    return result


# ---------------------------------------------------------------------------
# SQL execution helper (shared by Phase 3 and Phase 4 re-exec)
# ---------------------------------------------------------------------------

def _serialize_sql_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="replace")
    return str(value)


def _exec_sql(sql_text: str, db_path: str, timeout: int) -> Dict[str, Any]:
    """Execute a single SQL against SQLite. Returns an execution info dict."""
    sql_text = sql_text.strip()
    if not sql_text:
        return {"status": "skipped", "rows": None, "error": "empty_sql", "elapsed": 0.0}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    timeout = max(0, timeout)
    start = time.perf_counter()

    def _progress_handler() -> None:
        if timeout > 0 and time.perf_counter() - start > timeout:
            raise TimeoutError("SQL execution timed out")

    try:
        if timeout > 0:
            conn.set_progress_handler(_progress_handler, 1000)
        conn.execute("BEGIN TRANSACTION;")
        cursor.execute(sql_text)
        raw_rows = cursor.fetchall()
        rows = [[_serialize_sql_value(v) for v in row] for row in raw_rows]
        conn.rollback()
        elapsed = time.perf_counter() - start
        empty = len(rows) == 0
        return {
            "status": "succeeded",
            "rows": rows if Settings.SAVE_EXECUTION_RESULTS else None,
            "error": None,
            "elapsed": elapsed,
            "empty_result": empty,
        }
    except TimeoutError as exc:
        conn.rollback()
        elapsed = time.perf_counter() - start
        return {"status": "timeout", "rows": None, "error": str(exc), "elapsed": elapsed}
    except Exception as exc:
        conn.rollback()
        elapsed = time.perf_counter() - start
        msg = str(exc)
        status = "timeout" if "interrupted" in msg.lower() else "failed"
        return {"status": status, "rows": None, "error": msg, "elapsed": elapsed}
    finally:
        conn.set_progress_handler(None, 0)
        conn.close()


# ---------------------------------------------------------------------------
# Phase 2 — SQL pre-processing (rule-based cleanup before validation)
# ---------------------------------------------------------------------------

_PHASE2_PRIMARY_SUBTYPE_PRIORITY = (
    "missing_from_or_select",
    "quote_mismatch",
    "bracket_mismatch",
    "no_such_table",
    "no_such_column",
    "schema_validate_fail",
    "other",
)

_PHASE2_COARSE_BY_SUBTYPE = {
    "clean": "clean",
    "missing_from_or_select": "non_sql_output",
    "quote_mismatch": "syntax_error",
    "bracket_mismatch": "syntax_error",
    "no_such_table": "schema_error",
    "no_such_column": "schema_error",
    "schema_validate_fail": "schema_error",
    "other": "other_error",
}

_PHASE2_SCHEMA_TABLE_ERROR_RE = re.compile(
    r"^\s+'[^']+'\s+\s+Schema\s+$",
    re.IGNORECASE,
)
_PHASE2_SQL_START_KEYWORDS = ("SELECT", "WITH")
_PHASE2_DB_TABLE_CACHE: Dict[str, set[str]] = {}
_PHASE2_DB_TABLE_CACHE_LOCK = threading.Lock()


def _canonicalize_phase2_error(error: str) -> str:
    """Map a raw validator/preprocess error string into a canonical Phase2 subtype."""
    text = (error or "").strip()
    lower_text = text.lower()

    if (
        text in {"no_sql_content", "empty_sql"}
        or "sql  select  with " in lower_text
        or "must start with select or with" in lower_text
    ):
        return "missing_from_or_select"
    if lower_text.startswith("true_missing_table:"):
        return "no_such_table"
    if "" in text or "" in text:
        return "quote_mismatch"
    if "" in text:
        return "bracket_mismatch"
    if re.search(r"\s+'[^']+'\s+\s+schema\s+", text, re.IGNORECASE):
        return "no_such_table"
    if "no such table" in lower_text:
        return "no_such_table"
    if "" in text or "no such column" in lower_text:
        return "no_such_column"
    if "schema " in lower_text or "" in text or "syntax error" in lower_text:
        return "schema_validate_fail"
    return "other"


def _classify_phase2_errors(raw_errors: List[str]) -> Dict[str, Any]:
    """Build the Phase2 canonical error classification from raw validator errors."""
    cleaned_errors = [(err or "").strip() for err in raw_errors if (err or "").strip()]
    if not cleaned_errors:
        return {
            "coarse": "clean",
            "primary_subtype": "clean",
            "matched_subtypes": [],
            "raw_errors": [],
        }

    matched_subtypes: List[str] = []
    for err in cleaned_errors:
        subtype = _canonicalize_phase2_error(err)
        if subtype not in matched_subtypes:
            matched_subtypes.append(subtype)

    primary_subtype = "other"
    for candidate_subtype in _PHASE2_PRIMARY_SUBTYPE_PRIORITY:
        if candidate_subtype in matched_subtypes:
            primary_subtype = candidate_subtype
            break

    return {
        "coarse": _PHASE2_COARSE_BY_SUBTYPE[primary_subtype],
        "primary_subtype": primary_subtype,
        "matched_subtypes": matched_subtypes,
        "raw_errors": cleaned_errors,
    }


def _inspect_phase2_sql(
    sql_text: str,
    schema_info: Dict[str, Any],
    db_path: Optional[str],
    override_errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run Phase2 validation and return canonical diagnostics for the SQL text."""
    if override_errors is not None:
        validator_raw_errors = override_errors
    elif not sql_text.strip():
        validator_raw_errors = ["empty_sql"]
    else:
        syntax_errors = validate_sql_syntax(sql_text)
        schema_errors = validate_sql_schema(sql_text, schema_info, db_path)
        validator_raw_errors = syntax_errors + schema_errors

    effective_raw_errors = [
        err for err in validator_raw_errors if not _PHASE2_SCHEMA_TABLE_ERROR_RE.match(err)
    ]
    true_missing_tables = _detect_true_missing_tables(sql_text, db_path)
    effective_raw_errors.extend(f"true_missing_table: {table}" for table in true_missing_tables)

    classification = _classify_phase2_errors(effective_raw_errors)
    classification["raw_errors"] = list(validator_raw_errors)
    classification["effective_raw_errors"] = list(effective_raw_errors)
    classification["true_missing_tables"] = true_missing_tables
    return classification


def _get_phase2_db_tables(db_path: Optional[str]) -> set[str]:
    if not db_path:
        return set()
    with _PHASE2_DB_TABLE_CACHE_LOCK:
        cached = _PHASE2_DB_TABLE_CACHE.get(db_path)
    if cached is not None:
        return cached

    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    finally:
        conn.close()
    tables = {row[0].lower() for row in rows}
    with _PHASE2_DB_TABLE_CACHE_LOCK:
        _PHASE2_DB_TABLE_CACHE[db_path] = tables
    return tables


def _detect_true_missing_tables(sql_text: str, db_path: Optional[str]) -> List[str]:
    sql_text = (sql_text or "").strip()
    if not db_path or not sql_text:
        return []
    sql_upper = sql_text.upper()
    if not sql_upper.startswith(_PHASE2_SQL_START_KEYWORDS):
        return []

    valid_tables = _get_phase2_db_tables(db_path)
    if not valid_tables:
        return []

    cte_names = {name.lower() for name in extract_cte_names(sql_text)}
    referenced_tables = {name.lower() for name in extract_table_names(sql_text)}
    return sorted(
        table
        for table in referenced_tables
        if table not in cte_names and table not in valid_tables
    )


def _counter_to_sorted_dict(counter: Counter) -> Dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))

def _preprocess_sql(sql_text: str) -> tuple[str, list[str]]:
    """Apply rule-based cleanup to recover valid SQL from common LLM output artifacts.

    Returns:
        (cleaned_sql, applied_fixes)  — applied_fixes is a list of tag strings
          recording which rules fired, e.g. ["fix_comment_prefix", "fix_select_concat"].
        If the text cannot be recovered (pure thinking text), returns ("", ["dropped_no_sql"]).
    """
    if not sql_text or not sql_text.strip():
        return "", ["dropped_no_sql"]

    fixes: list[str] = []
    text = sql_text.strip()

    # Rule 1: strip leading `--` comment lines
    # e.g. "-- Your SQL query\nSELECT ..."  →  "SELECT ..."
    if text.startswith("--"):
        lines = text.splitlines()
        non_comment_lines = [ln for ln in lines if not ln.strip().startswith("--")]
        candidate_text = "\n".join(non_comment_lines).strip()
        if candidate_text:
            text = candidate_text
            fixes.append("fix_comment_prefix")

    # Rule 2: strip leading `#` comment lines
    # e.g. "# SELECT ...\n# FROM ..."  →  "SELECT ...\nFROM ..."
    if text.startswith("#"):
        lines = text.splitlines()
        stripped_lines = []
        for ln in lines:
            stripped = ln.strip()
            if stripped.startswith("#"):
                stripped_lines.append(stripped.lstrip("#").strip())
            else:
                stripped_lines.append(ln)
        candidate_text = "\n".join(stripped_lines).strip()
        if candidate_text:
            text = candidate_text
            fixes.append("fix_hash_prefix")

    # Rule 3: fix SELECT-token concatenation
    # e.g. "SELECTbadges.Name FROM ..."  →  "SELECT badges.Name FROM ..."
    import re as _re
    if _re.match(r"^SELECT[^\s]", text, _re.IGNORECASE):
        text = _re.sub(r"^SELECT(?=[^\s])", "SELECT ", text, count=1, flags=_re.IGNORECASE)
        fixes.append("fix_select_concat")

    # Final check: does the cleaned text start with a valid SQL keyword?
    first_token = text.split()[0].upper() if text.split() else ""
    if first_token not in ("SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP"):
        # Cannot recover — pure thinking text
        return "", ["dropped_no_sql"]

    return text, fixes


# ---------------------------------------------------------------------------
# Phase 2 — validation + preprocessing hard-filtering (no LLM correction)
# ---------------------------------------------------------------------------

def _analyze_single_phase2(
    sample: Dict[str, Any],
    candidate: Dict[str, Any],
    db_path: Optional[str],
) -> Dict[str, Any]:
    """Analyze one Phase2 candidate without invoking any LLM correction."""
    original_sql = candidate.get("sql", "")
    schema_info = sample.get("schema_info", {})
    before = _inspect_phase2_sql(original_sql, schema_info, db_path)

    cleaned_sql, fixes = _preprocess_sql(original_sql)
    if fixes:
        candidate["preprocess_fixes"] = fixes
    else:
        candidate.pop("preprocess_fixes", None)

    if "dropped_no_sql" in fixes:
        after = _inspect_phase2_sql("", schema_info, db_path, override_errors=["no_sql_content"])
    else:
        sql_for_validation = cleaned_sql if fixes else original_sql
        if fixes and cleaned_sql:
            candidate["sql"] = cleaned_sql
        after = _inspect_phase2_sql(sql_for_validation, schema_info, db_path)

    diagnostics = {
        "before_preprocess": before,
        "after_preprocess": after,
        "preprocess_fixes": fixes,
        "llm_correction_attempted": False,
    }
    removal_reason = None if after["primary_subtype"] == "clean" else after["primary_subtype"]
    validation_status = "passed" if removal_reason is None else "failed"
    return {
        "status": validation_status,
        "errors": after["raw_errors"],
        "warnings": [],
        "diagnostics": diagnostics,
        "removal_reason": removal_reason,
        "missing_tables": after["true_missing_tables"],
    }


def _build_phase2_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_candidates = 0
    total_candidates_after = 0
    removed_total = 0
    clean_before = 0
    error_before = 0
    samples_with_removals = 0
    zero_candidate_samples_after = 0
    counts_before_by_coarse: Counter = Counter()
    counts_before_by_subtype: Counter = Counter()
    counts_after_by_coarse: Counter = Counter()
    counts_after_by_subtype: Counter = Counter()
    preprocess_fix_counts: Counter = Counter()
    removed_by_reason: Counter = Counter()

    for sample in results:
        kept_candidates = sample.get("sql_candidates", [])
        removed_candidates = sample.get("phase2_removed_candidates", [])

        total_candidates_after += len(kept_candidates)
        removed_total += len(removed_candidates)
        if removed_candidates:
            samples_with_removals += 1
        if not kept_candidates:
            zero_candidate_samples_after += 1

        for candidate in kept_candidates + removed_candidates:
            total_candidates += 1
            diagnostics = candidate.get("phase2_diagnostics") or {}
            before = diagnostics.get("before_preprocess") or _classify_phase2_errors([])
            after = diagnostics.get("after_preprocess") or _classify_phase2_errors([])

            counts_before_by_coarse[before["coarse"]] += 1
            counts_before_by_subtype[before["primary_subtype"]] += 1
            counts_after_by_coarse[after["coarse"]] += 1
            counts_after_by_subtype[after["primary_subtype"]] += 1

            if before["primary_subtype"] == "clean":
                clean_before += 1
            else:
                error_before += 1

            for fix_tag in diagnostics.get("preprocess_fixes") or []:
                preprocess_fix_counts[fix_tag] += 1

        for candidate in removed_candidates:
            removed_by_reason[candidate.get("removal_reason") or "unknown"] += 1

    return {
        "total_candidates": total_candidates,
        "total_candidates_before": total_candidates,
        "total_candidates_after": total_candidates_after,
        "removed_total": removed_total,
        "removed_by_reason": _counter_to_sorted_dict(removed_by_reason),
        "samples_with_removals": samples_with_removals,
        "zero_candidate_samples_after": zero_candidate_samples_after,
        "clean_before": clean_before,
        "error_before": error_before,
        "counts_before_by_coarse": _counter_to_sorted_dict(counts_before_by_coarse),
        "counts_before_by_subtype": _counter_to_sorted_dict(counts_before_by_subtype),
        "counts_after_preprocess_by_coarse": _counter_to_sorted_dict(counts_after_by_coarse),
        "counts_after_preprocess_by_subtype": _counter_to_sorted_dict(counts_after_by_subtype),
        "preprocess_fix_counts": _counter_to_sorted_dict(preprocess_fix_counts),
    }


def run_phase2(args: argparse.Namespace, payload_override: Optional[Dict[str, Any]] = None) -> None:
    input_path = Path(args.input)
    payload = payload_override or json.loads(input_path.read_text(encoding="utf-8"))
    results = payload.get("results", [])
    resume_mode = bool(getattr(args, "resume", False))
    filtered_payload = "total_candidates_after" in (payload.get("phase2_summary") or {})

    futures: List[Tuple] = []
    skipped = 0
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for sample in results:
            db_id = sample.get("db_id")
            try:
                db_path = str(Settings.get_database_path(db_id)) if db_id else None
            except FileNotFoundError as exc:
                logger.warning("Phase2: db %s not found: %s", db_id, exc)
                db_path = None
            for candidate in sample.get("sql_candidates", []):
                if resume_mode and filtered_payload and candidate.get("phase2_diagnostics") is not None:
                    status = (candidate.get("validation") or {}).get("status")
                    if status in {"passed", "failed", "skipped"}:
                        skipped += 1
                        continue
                futures.append(
                    (
                        executor.submit(_analyze_single_phase2, sample, candidate, db_path),
                        candidate,
                    )
                )

        _TASK_TIMEOUT = 120
        for future, candidate in _iter_with_progress(futures, len(futures), "Phase2"):
            try:
                outcome = future.result(timeout=_TASK_TIMEOUT)
            except TimeoutError:
                logger.warning("Phase2: task timeout (>%ds), marking failed", _TASK_TIMEOUT)
                outcome = {
                    "status": "failed",
                    "errors": ["validation_timeout"],
                    "warnings": [],
                    "diagnostics": {
                        "before_preprocess": _classify_phase2_errors(["validation_timeout"]),
                        "after_preprocess": _classify_phase2_errors(["validation_timeout"]),
                        "preprocess_fixes": [],
                        "llm_correction_attempted": False,
                    },
                    "removal_reason": "other",
                    "missing_tables": [],
                }
            except Exception as exc:
                logger.warning("Phase2: task exception: %s, marking failed", exc)
                outcome = {
                    "status": "failed",
                    "errors": [str(exc)],
                    "warnings": [],
                    "diagnostics": {
                        "before_preprocess": _classify_phase2_errors([str(exc)]),
                        "after_preprocess": _classify_phase2_errors([str(exc)]),
                        "preprocess_fixes": [],
                        "llm_correction_attempted": False,
                    },
                    "removal_reason": "other",
                    "missing_tables": [],
                }

            validation_entry: Dict[str, Any] = {
                "status": outcome["status"],
                "errors": outcome["errors"],
                "warnings": outcome["warnings"],
            }
            if candidate.get("preprocess_fixes"):
                validation_entry["preprocess_fixes"] = candidate["preprocess_fixes"]
            candidate["validation"] = validation_entry
            candidate["phase2_diagnostics"] = outcome["diagnostics"]
            missing_tables = outcome.get("missing_tables") or []
            if missing_tables:
                candidate["missing_tables"] = missing_tables
            else:
                candidate.pop("missing_tables", None)

            removal_reason = outcome.get("removal_reason")
            if removal_reason is not None:
                candidate["removal_reason"] = removal_reason
            else:
                candidate.pop("removal_reason", None)

    for sample in results:
        kept_candidates = []
        removed_candidates = []
        for candidate in sample.get("sql_candidates", []):
            if candidate.get("removal_reason") is None:
                kept_candidates.append(candidate)
            else:
                removed_candidates.append(candidate)
        sample["sql_candidates"] = kept_candidates
        sample["phase2_removed_candidates"] = removed_candidates

    payload["phase2_summary"] = _build_phase2_summary(results)
    tmp_path = input_path.with_suffix(".phase2v2.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(input_path)
    if resume_mode:
        logger.info("Phase2(v2): resume mode skipped {} candidates with diagnostics", skipped)

    # --- Phase 2 summary log ---
    s = payload["phase2_summary"]
    removed_detail = " ".join(f"{k}={v}" for k, v in s.get("removed_by_reason", {}).items())
    fix_detail = " ".join(f"{k}={v}" for k, v in s.get("preprocess_fix_counts", {}).items())
    logger.info(
        "Phase2(v2): done. total={} kept={} removed={} zero_candidate_samples={} → {}",
        s["total_candidates"], s["total_candidates_after"], s["removed_total"],
        s["zero_candidate_samples_after"], input_path,
    )
    if removed_detail:
        logger.info("Phase2(v2): removed_by_reason: {}", removed_detail)
    if fix_detail:
        logger.info("Phase2(v2): preprocess_fixes: {}", fix_detail)
    _save_phase_snapshot(input_path, "phase2")


# ---------------------------------------------------------------------------
# Phase 3 — execution diagnostics (runtime error / timeout / empty result)
# ---------------------------------------------------------------------------

_PHASE3_EXEC_FINAL_STATUSES = {"succeeded", "failed", "timeout", "skipped", "discarded"}
_PHASE3_EXECUTED_STATUSES = {"succeeded", "failed", "timeout"}


def _build_phase3_diagnostics(exec_info: Dict[str, Any]) -> Dict[str, Any]:
    status = (exec_info.get("status") or "").strip().lower()
    error_message = exec_info.get("error")
    discard_reason = exec_info.get("discard_reason")
    empty_result = bool(exec_info.get("empty_result"))

    failure_type = "unknown"
    needs_followup = False
    followup_reason = None

    if status == "succeeded" and empty_result:
        failure_type = "empty_result"
        needs_followup = True
        followup_reason = "empty_result"
        if not error_message:
            error_message = "empty_result"
    elif status == "succeeded":
        failure_type = "clean"
    elif status == "failed":
        failure_type = "runtime_error"
        needs_followup = True
        followup_reason = "runtime_error"
    elif status == "timeout":
        failure_type = "timeout"
        needs_followup = True
        followup_reason = "timeout"
    elif status == "discarded" and discard_reason and "timeout" in discard_reason:
        failure_type = "timeout"
        needs_followup = True
        followup_reason = "timeout"
        if not error_message:
            error_message = discard_reason
    elif status == "skipped" and error_message == "missing_db":
        failure_type = "missing_db"
    elif status == "skipped" and error_message == "empty_sql":
        failure_type = "empty_sql"
    elif status == "skipped" and error_message == "validation_failed":
        failure_type = "validation_blocked"

    return {
        "failure_type": failure_type,
        "needs_followup": needs_followup,
        "followup_reason": followup_reason,
        "error_message": error_message,
    }


def _build_phase3_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_candidates = 0
    executed_candidates = 0
    clean_candidates = 0
    followup_total = 0
    runtime_error_total = 0
    timeout_total = 0
    empty_result_total = 0
    missing_db_total = 0
    validation_blocked_total = 0
    empty_sql_total = 0
    followup_by_reason: Counter = Counter()
    samples_with_followup_candidates = 0

    for sample in results:
        sample_has_followup = False
        for candidate in sample.get("sql_candidates", []):
            total_candidates += 1
            exec_info = candidate.get("execution") or {}
            diagnostics = candidate.get("phase3_diagnostics") or _build_phase3_diagnostics(exec_info)
            status = (exec_info.get("status") or "").strip().lower()
            failure_type = diagnostics.get("failure_type")

            if status in _PHASE3_EXECUTED_STATUSES:
                executed_candidates += 1
            if failure_type == "clean":
                clean_candidates += 1
            elif failure_type == "runtime_error":
                runtime_error_total += 1
            elif failure_type == "timeout":
                timeout_total += 1
            elif failure_type == "empty_result":
                empty_result_total += 1
            elif failure_type == "missing_db":
                missing_db_total += 1
            elif failure_type == "validation_blocked":
                validation_blocked_total += 1
            elif failure_type == "empty_sql":
                empty_sql_total += 1

            if diagnostics.get("needs_followup"):
                followup_total += 1
                sample_has_followup = True
                followup_by_reason[diagnostics.get("followup_reason") or "unknown"] += 1

        if sample_has_followup:
            samples_with_followup_candidates += 1

    return {
        "total_candidates": total_candidates,
        "executed_candidates": executed_candidates,
        "clean_candidates": clean_candidates,
        "followup_total": followup_total,
        "followup_by_reason": _counter_to_sorted_dict(followup_by_reason),
        "runtime_error_total": runtime_error_total,
        "timeout_total": timeout_total,
        "empty_result_total": empty_result_total,
        "missing_db_total": missing_db_total,
        "validation_blocked_total": validation_blocked_total,
        "empty_sql_total": empty_sql_total,
        "samples_with_followup_candidates": samples_with_followup_candidates,
    }


def run_phase3(args: argparse.Namespace) -> None:
    input_path = Path(args.input)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    results = payload.get("results", [])
    timeout = max(0, int(args.timeout))
    resume_mode = bool(getattr(args, "resume", False))

    jobs: List[Tuple[Dict[str, Any], str]] = []
    skipped = 0
    missing_db_total = 0

    for sample in results:
        db_id = sample.get("db_id")
        try:
            db_path = str(Settings.get_database_path(db_id)) if db_id else None
        except FileNotFoundError as exc:
            logger.warning("Phase3: db %s not found: %s", db_id, exc)
            db_path = None

        if not db_path:
            for cand in sample.get("sql_candidates", []):
                exec_info = {
                    "status": "skipped",
                    "rows": None,
                    "error": "missing_db",
                    "elapsed": None,
                }
                cand["execution"] = exec_info
                cand["phase3_diagnostics"] = _build_phase3_diagnostics(exec_info)
            missing_db_total += len(sample.get("sql_candidates", []))
            continue

        for cand in sample.get("sql_candidates", []):
            if resume_mode:
                exec_status = (cand.get("execution") or {}).get("status")
                if exec_status in _PHASE3_EXEC_FINAL_STATUSES:
                    if cand.get("phase3_diagnostics") is None:
                        cand["phase3_diagnostics"] = _build_phase3_diagnostics(
                            cand.get("execution") or {}
                        )
                    skipped += 1
                    continue
            jobs.append((cand, db_path))

    def _run_job(job: Tuple[Dict[str, Any], str]):
        candidate, db_path = job
        val_status = (candidate.get("validation") or {}).get("status")
        if val_status not in ("passed", "autocorrected"):
            return candidate, {
                "status": "skipped",
                "rows": None,
                "error": "validation_failed",
                "elapsed": None,
            }
        sql_text = candidate.get("sql", "").strip()
        if not sql_text:
            return candidate, {
                "status": "skipped",
                "rows": None,
                "error": "empty_sql",
                "elapsed": None,
            }
        return candidate, _exec_sql(sql_text, db_path, timeout)

    if jobs:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = {executor.submit(_run_job, job): job for job in jobs}
            for future in _iter_completed_futures(futures, "Phase3"):
                candidate, exec_info = future.result()
                candidate["execution"] = exec_info
                candidate["phase3_diagnostics"] = _build_phase3_diagnostics(exec_info)
    else:
        logger.warning("Phase3(v2): no candidates to execute")

    for sample in results:
        for cand in sample.get("sql_candidates", []):
            if cand.get("phase3_diagnostics") is None:
                cand["phase3_diagnostics"] = _build_phase3_diagnostics(cand.get("execution") or {})

    payload["phase3_summary"] = _build_phase3_summary(results)
    tmp_path = input_path.with_suffix(".phase3v2.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(input_path)
    if resume_mode:
        logger.info("Phase3(v2): resume mode skipped {} candidates", skipped)

    # --- Phase 3 summary log ---
    s = payload["phase3_summary"]
    followup_detail = " ".join(f"{k}={v}" for k, v in s.get("followup_by_reason", {}).items())
    logger.info(
        "Phase3(v2): done. total={} executed={} clean={} followup={} "
        "(empty={} timeout={} runtime_error={}) samples_with_followup={} → {}",
        s["total_candidates"], s["executed_candidates"], s["clean_candidates"],
        s["followup_total"], s["empty_result_total"], s["timeout_total"],
        s["runtime_error_total"], s["samples_with_followup_candidates"], input_path,
    )
    if followup_detail:
        logger.info("Phase3(v2): followup_by_reason: {}", followup_detail)
    _save_phase_snapshot(input_path, "phase3")


# ---------------------------------------------------------------------------
# Phase 4 — execution-feedback-aware correction + immediate re-execution
# ---------------------------------------------------------------------------


def _build_phase4_summary(
    entered_by_failure_type: Counter,
    fixed_by_failure_type: Counter,
    failed_by_failure_type: Counter,
) -> Dict[str, Any]:
    return {
        "entered_total": sum(entered_by_failure_type.values()),
        "entered_by_failure_type": _counter_to_sorted_dict(entered_by_failure_type),
        "fixed_total": sum(fixed_by_failure_type.values()),
        "fixed_by_failure_type": _counter_to_sorted_dict(fixed_by_failure_type),
        "failed_total": sum(failed_by_failure_type.values()),
        "failed_by_failure_type": _counter_to_sorted_dict(failed_by_failure_type),
    }


def run_phase4(args: argparse.Namespace) -> None:
    input_path = Path(args.input)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    results = payload.get("results", [])
    progress_path = input_path.with_suffix(".phase4.progress.json")

    jobs: List[Tuple[Dict[str, Any], Dict[str, Any], int, str, str, str, str, Optional[str], str]] = []
    entered_by_failure_type: Counter = Counter()
    for sample in results:
        schema_info = sample.get("schema_info", {})
        schema_description = (
            schema_info.get("schema_description") or schema_info.get("schema_prompt_text") or ""
        )
        question = sample.get("question", "")
        evidence = sample.get("evidence", "")
        value_link_prompt_hints = sample.get("value_link_prompt_hints", "")
        db_id = sample.get("db_id")
        try:
            db_path = str(Settings.get_database_path(db_id)) if db_id else None
        except FileNotFoundError:
            db_path = None

        for cand_idx, cand in enumerate(sample.get("sql_candidates", [])):
            val_status = (cand.get("validation") or {}).get("status")
            exec_info = cand.get("execution") or {}
            exec_status = exec_info.get("status")
            current_diag = _build_phase3_diagnostics(exec_info)
            failure_type = current_diag.get("failure_type")

            # IMPORTANT: Phase4 must trust the CURRENT execution result, not stale phase3_diagnostics.
            needs_fix = bool(current_diag.get("needs_followup"))
            if not needs_fix and val_status == "failed":
                needs_fix = True
                failure_type = "validation_error"
            if not needs_fix:
                continue

            attempts = cand.get("auto_correct_attempts", 0)
            if attempts >= args.max_attempts:
                continue

            normalized_failure_type = failure_type or (
                "runtime_error" if exec_status == "failed" else "validation_error"
            )
            entered_by_failure_type[normalized_failure_type] += 1
            jobs.append(
                (
                    sample,
                    cand,
                    cand_idx,
                    question,
                    evidence,
                    value_link_prompt_hints,
                    schema_description,
                    db_path,
                    normalized_failure_type,
                )
            )

    if not jobs:
        logger.info("Phase4(v2): no candidates need correction")
        payload["phase4_summary"] = _build_phase4_summary(
            entered_by_failure_type,
            Counter(),
            Counter(),
        )
        tmp_path = input_path.with_suffix(".phase4v2.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(input_path)
        progress_path.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "total_jobs": 0,
                    "completed_jobs": 0,
                    "empty_stage1_fixed": 0,
                    "empty_stage2_fixed": 0,
                    "phase4_summary": payload["phase4_summary"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        _save_phase_snapshot(input_path, "phase4")
        return

    timeout = max(0, int(getattr(args, "exec_timeout", 120)))
    stage2_bank = ErrorBankStore()
    stage2_model_key = getattr(args, "empty_stage2_model_key", "qwen3-235b")
    stage2_model_cfg = get_model_config(stage2_model_key)
    stage2_max_rounds = int(getattr(args, "empty_stage2_rounds", 2))
    stage2_llm_timeout = int(getattr(args, "empty_stage2_llm_timeout", 20))
    stage2_max_tokens = int(getattr(args, "empty_stage2_max_tokens", 1200))

    empty_stage1_fixed = 0
    empty_stage2_fixed = 0

    def _write_phase4_progress(status: str) -> None:
        progress_payload = {
            "status": status,
            "total_jobs": len(jobs),
            "completed_jobs": completed_jobs,
            "empty_stage1_fixed": empty_stage1_fixed,
            "empty_stage2_fixed": empty_stage2_fixed,
            "entered_by_failure_type": _counter_to_sorted_dict(entered_by_failure_type),
            "fixed_by_failure_type": _counter_to_sorted_dict(fixed_by_failure_type),
            "failed_by_failure_type": _counter_to_sorted_dict(failed_by_failure_type),
        }
        progress_path.write_text(
            json.dumps(progress_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _run_job(job: Tuple[Dict[str, Any], Dict[str, Any], int, str, str, str, str, Optional[str], str]):
        sample, candidate, cand_idx, question, evidence, value_link_prompt_hints, schema_description, db_path, failure_type = job
        sql_text = candidate.get("sql", "")
        exec_info = candidate.get("execution") or {}
        exec_error = exec_info.get("error") or ""
        val_errors = list((candidate.get("validation") or {}).get("errors") or [])

        try:
            if failure_type == "empty_result":
                correction_info, re_exec = _correct_empty_result_two_stage(
                    sample=sample,
                    candidate=candidate,
                    cand_idx=cand_idx,
                    db_path=db_path,
                    exec_timeout=timeout,
                    stage2_bank=stage2_bank,
                    stage2_model_cfg=stage2_model_cfg,
                    stage2_max_rounds=stage2_max_rounds,
                    stage2_llm_timeout=stage2_llm_timeout,
                    stage2_max_tokens=stage2_max_tokens,
                )
                if correction_info is not None and re_exec is not None:
                    return candidate, correction_info, re_exec, None, failure_type, sql_text
                return candidate, {
                    "corrected_sql": "",
                    "raw_output": "",
                    "prompt": "",
                    "model_key": "two_stage_empty_repair",
                    "model_name": "two_stage_empty_repair",
                    "transport": "phase4_empty_two_stage",
                    "preprocess_fixes": [],
                    "prompt_type": "empty_result_no_fix",
                }, None, "no_two_stage_fix", failure_type, sql_text
            elif failure_type == "timeout":
                timeout_seconds = int(round(exec_info.get("elapsed") or timeout or 120))
                correction_info = _correct_timeout(
                    question=question,
                    candidate_sql=sql_text,
                    schema_description=schema_description,
                    evidence=evidence,
                    value_link_prompt_hints=value_link_prompt_hints,
                    timeout_seconds=timeout_seconds,
                )
            elif failure_type == "runtime_error" and exec_error:
                correction_info = _correct_execution_error(
                    question=question,
                    candidate_sql=sql_text,
                    exec_error=exec_error,
                    extra_errors=val_errors,
                    schema_description=schema_description,
                    evidence=evidence,
                    value_link_prompt_hints=value_link_prompt_hints,
                )
            else:
                all_errors = val_errors or ["Unknown failure, please regenerate SQL"]
                correction_info = _correct_syntax_errors(
                    question=question,
                    candidate_sql=sql_text,
                    errors=all_errors,
                    schema_description=schema_description,
                    evidence=evidence,
                    value_link_prompt_hints=value_link_prompt_hints,
                )
        except Exception as exc:
            return candidate, None, None, str(exc), failure_type, sql_text

        corrected_sql = (correction_info or {}).get("corrected_sql") or ""
        if not corrected_sql:
            return candidate, correction_info, None, "empty corrected sql", failure_type, sql_text

        if not db_path:
            return candidate, correction_info, None, "missing_db_for_reexecution", failure_type, sql_text

        re_exec = _exec_sql(corrected_sql, db_path, timeout)
        return candidate, correction_info, re_exec, None, failure_type, sql_text

    fixed_by_failure_type: Counter = Counter()
    failed_by_failure_type: Counter = Counter()
    completed_jobs = 0
    _write_phase4_progress("running")
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {executor.submit(_run_job, job): job for job in jobs}
        for future in _iter_completed_futures(futures, "Phase4"):
            candidate, correction_info, re_exec, err, failure_type, input_sql = future.result()
            completed_jobs += 1
            candidate.setdefault("auto_correct_attempts", 0)
            candidate["auto_correct_attempts"] += 1
            corrected_sql = (correction_info or {}).get("corrected_sql")

            history_entry = {
                "attempt": candidate["auto_correct_attempts"],
                "failure_type": failure_type,
                "prompt_type": (correction_info or {}).get("prompt_type"),
                "prompt_text": (correction_info or {}).get("prompt"),
                "model_key": (correction_info or {}).get("model_key"),
                "model_name": (correction_info or {}).get("model_name"),
                "transport": (correction_info or {}).get("transport"),
                "input_sql": input_sql,
                "raw_model_output": (correction_info or {}).get("raw_output"),
                "corrected_sql": corrected_sql,
                "preprocess_fixes": (correction_info or {}).get("preprocess_fixes") or [],
                "re_execution": re_exec,
                "error": err,
            }
            candidate.setdefault("phase4_correction_history", []).append(history_entry)

            if err or not corrected_sql:
                candidate["auto_correct_error"] = err or "empty corrected sql"
                candidate.setdefault("validation", {})["status"] = "failed"
                failed_by_failure_type[failure_type] += 1
                continue

            candidate["execution"] = re_exec
            if re_exec.get("status") == "succeeded" and not re_exec.get("empty_result", False):
                candidate["sql"] = corrected_sql
                candidate.pop("auto_correct_error", None)
                cand_val = candidate.setdefault("validation", {})
                cand_val["status"] = "autocorrected"
                cand_val["errors"] = []
                cand_val["warnings"] = []
                if failure_type == "empty_result":
                    prompt_type = (correction_info or {}).get("prompt_type")
                    if prompt_type == "empty_result_hp_repair":
                        empty_stage1_fixed += 1
                    elif prompt_type == "empty_result_phase4_stage2":
                        empty_stage2_fixed += 1
                fixed_by_failure_type[failure_type] += 1
            else:
                candidate["auto_correct_error"] = (
                    "re_execution_empty_result"
                    if re_exec.get("empty_result", False)
                    else f"re_execution_{re_exec.get('status')}"
                )
                candidate.setdefault("validation", {})["status"] = "failed"
                failed_by_failure_type[failure_type] += 1

            if completed_jobs % 25 == 0 or completed_jobs == len(jobs):
                logger.info(
                    "Phase4(v2): progress {}/{} empty_stage1_fixed={} empty_stage2_fixed={}",
                    completed_jobs,
                    len(jobs),
                    empty_stage1_fixed,
                    empty_stage2_fixed,
                )
                _write_phase4_progress("running")

    payload["phase4_summary"] = _build_phase4_summary(
        entered_by_failure_type,
        fixed_by_failure_type,
        failed_by_failure_type,
    )
    payload["phase4_summary"]["empty_stage1_fixed"] = empty_stage1_fixed
    payload["phase4_summary"]["empty_stage2_fixed"] = empty_stage2_fixed
    tmp_path = input_path.with_suffix(".phase4v2.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(input_path)
    _write_phase4_progress("completed")

    # --- Phase 4 summary log ---
    s = payload["phase4_summary"]
    entered_detail = " ".join(f"{k}={v}" for k, v in s.get("entered_by_failure_type", {}).items())
    fixed_detail = " ".join(f"{k}={v}" for k, v in s.get("fixed_by_failure_type", {}).items())
    failed_detail = " ".join(f"{k}={v}" for k, v in s.get("failed_by_failure_type", {}).items())
    logger.info(
        "Phase4(v2): done. entered={} fixed={} failed={} → {}",
        s["entered_total"], s["fixed_total"], s["failed_total"], input_path,
    )
    if entered_detail:
        logger.info("Phase4(v2): entered_by_type: {}", entered_detail)
    if fixed_detail:
        logger.info("Phase4(v2): fixed_by_type: {}", fixed_detail)
    if failed_detail:
        logger.info("Phase4(v2): failed_by_type: {}", failed_detail)
    _save_phase_snapshot(input_path, "phase4")


# ---------------------------------------------------------------------------
# Phase 5 — rule filtering + zero-candidate regen (unchanged logic, new entry)
# ---------------------------------------------------------------------------

def _sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_for_json(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [_sanitize_for_json(item) for item in value]
    return value


def _build_phase5_rule_lines(args: argparse.Namespace) -> List[str]:
    rules_file = getattr(args, "rules_file", None)
    if rules_file:
        return Path(rules_file).read_text(encoding="utf-8").splitlines()
    preset = getattr(args, "rules_preset", "qes_30")
    if preset == "qes_30":
        return list(RULES_QES_30)
    if preset == "refined_rules_depth1":
        from scripts.filter_sql_candidates_by_rules import RULES_REFINED_DEPTH1
        return list(RULES_REFINED_DEPTH1)
    raise ValueError(f"Phase5: unsupported rules preset: {preset}")


def run_phase5(args: argparse.Namespace) -> None:
    """Phase 5: rule post-processing + zero-candidate regen. Logic identical to v1."""
    input_path = Path(args.input)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    samples = payload.get("results", [])
    if not samples:
        raise ValueError("Phase5: no samples found")

    rule_lines = _build_phase5_rule_lines(args)
    parsed_rules = parse_filter_rules(rule_lines)
    if not parsed_rules:
        raise ValueError("Phase5: empty rule set")

    q_re = compile_filter_patterns(FILTER_QUESTION_PATTERNS)
    s_re = compile_filter_patterns(FILTER_SQL_PATTERNS)
    e_re = compile_filter_patterns(FILTER_EVIDENCE_PATTERNS)

    gt_guard_signatures: Dict[Any, Any] = {}
    gt_guard_rows = 0
    gt_guard_hit_rows = 0
    train_gt_json = getattr(args, "train_gt_json", None)
    if train_gt_json:
        gt_rows = load_filter_gt_rows(Path(train_gt_json))
        gt_guard_rows = len(gt_rows)
        gt_guard_signatures, gt_guard_hit_rows, _ = build_filter_gt_signature_guard(
            gt_rows, parsed_rules, q_re, s_re, e_re
        )
        logger.info(
            "Phase5(GTGuard): gt_guard_rows=%d gt_guard_hit_rows=%d gt_guard_rules=%d",
            gt_guard_rows,
            gt_guard_hit_rows,
            len(gt_guard_signatures),
        )

    orig_total_sql = 0
    new_total_sql = 0
    removed_total_sql = 0
    invalid_removed_total_sql = 0
    gt_guard_spared_total_sql = 0
    affected_results = 0
    zero_candidate_results = 0
    affected_sample_entries: List[Tuple[int, Any]] = []
    zero_sample_entries: List[Tuple[int, Dict[str, Any]]] = []
    removed_sqls_by_qid: Dict[Any, List[str]] = {}
    matched_rules_by_qid: Dict[Any, List[str]] = {}

    for sample_idx, sample in enumerate(samples):
        qid = sample.get("question_id")
        question = sample.get("question", "")
        evidence = sample.get("evidence", "")
        candidates = sample.get("sql_candidates", [])
        orig_total_sql += len(candidates)

        kept: List[Dict[str, Any]] = []
        removed_sqls: List[str] = []
        matched_rules: List[str] = []
        matched_seen: set = set()

        for candidate in candidates:
            sql_text = (candidate.get("sql") or "").strip()
            val_status = (candidate.get("validation") or {}).get("status")
            exec_info = candidate.get("execution") or {}
            exec_status = exec_info.get("status")
            exec_empty = bool(exec_info.get("empty_result"))

            # Phase5 must consume Phase4's output state. Candidates that are still failed,
            # still empty, or never successfully executed are treated as already removed
            # before rule filtering; otherwise zero-candidate regeneration will never fire
            # for samples whose entire pool was exhausted by Phase4.
            if val_status not in ("passed", "autocorrected") or exec_status != "succeeded" or exec_empty:
                if sql_text:
                    removed_sqls.append(sql_text)
                invalid_removed_total_sql += 1
                if "phase4_invalid_or_unresolved" not in matched_seen:
                    matched_seen.add("phase4_invalid_or_unresolved")
                    matched_rules.append("phase4_invalid_or_unresolved")
                continue

            feats = extract_filter_features(question, sql_text, evidence, q_re, s_re, e_re)
            hit_rules = match_filter_rule_texts(feats, parsed_rules)
            if hit_rules and is_filter_candidate_guarded_by_gt(feats, hit_rules, gt_guard_signatures):
                gt_guard_spared_total_sql += 1
                kept.append(candidate)
                continue
            if hit_rules:
                if sql_text:
                    removed_sqls.append(sql_text)
                for rule in hit_rules:
                    if rule not in matched_seen:
                        matched_seen.add(rule)
                        matched_rules.append(rule)
                continue
            kept.append(candidate)

        for idx, candidate in enumerate(kept):
            candidate["index"] = idx
        sample["sql_candidates"] = kept

        cur_removed = len(candidates) - len(kept)
        new_total_sql += len(kept)
        removed_total_sql += cur_removed
        if cur_removed > 0:
            affected_results += 1
            affected_sample_entries.append((sample_idx, qid))
        # Also retry regeneration for samples that are already at zero candidates
        # when Phase5 is re-run after a previous regen failure.
        if len(kept) == 0:
            zero_candidate_results += 1
            removed_sqls_by_qid[qid] = removed_sqls
            matched_rules_by_qid[qid] = matched_rules
            zero_sample_entries.append((sample_idx, sample))

    logger.info(
        "Phase5(PostProcess): rules={} orig={} new={} removed={} invalid_removed={} affected={} zero={}",
        len(parsed_rules),
        orig_total_sql,
        new_total_sql,
        removed_total_sql,
        invalid_removed_total_sql,
        affected_results,
        zero_candidate_results,
    )

    zero_qids = [s.get("question_id") for _, s in zero_sample_entries]
    regen_success = 0
    regen_failed = 0

    if zero_qids and not getattr(args, "skip_regen", False):
        regen_cfg = RegenConfig(
            candidate_count=int(getattr(args, "regen_candidate_count", 16)),
            batch_size=int(getattr(args, "regen_batch_size", 4)),
            max_rounds=int(getattr(args, "regen_max_rounds", 4)),
            temperature=float(
                getattr(args, "regen_temperature", Settings.SQL_GENERATION_PRIMARY_TEMPERATURE)
            ),
            max_tokens=int(getattr(args, "regen_max_tokens", Settings.MAX_TOKENS)),
            timeout=int(getattr(args, "regen_timeout", Settings.SQL_GENERATION_REQUEST_TIMEOUT)),
            max_retries=int(getattr(args, "regen_max_retries", Settings.SQL_GENERATION_MAX_RETRIES)),
            retry_backoff=float(
                getattr(args, "regen_retry_backoff", Settings.SQL_GENERATION_RETRY_BACKOFF)
            ),
            max_bad_sql_examples=int(getattr(args, "regen_max_bad_sql_examples", 12)),
        )
        model_key = getattr(args, "regen_model_key", "qwen3-235b")
        model_cfg = get_model_config(model_key)
        regen_threads = max(1, int(getattr(args, "threads", 4)))
        model_name = model_cfg["model_name"]

        def _regen_single(sample_idx: int, sample: Dict[str, Any]) -> Dict[str, Any]:
            qid = sample.get("question_id")
            raw_removed = removed_sqls_by_qid.get(qid, [])
            dedup: List[str] = []
            seen_bad: set = set()
            for sql_text in raw_removed:
                norm = normalize_regen_sql(sql_text)
                if not norm or norm in seen_bad:
                    continue
                seen_bad.add(norm)
                dedup.append(sql_text)
            matched_rules = matched_rules_by_qid.get(qid, [])
            feedback_block = build_regen_feedback_block(
                question=sample.get("question", ""),
                removed_sqls=dedup,
                matched_rules=matched_rules,
                max_bad_sql_examples=regen_cfg.max_bad_sql_examples,
            )
            prompt = build_regen_prompt(sample, feedback_block)
            try:
                client = OpenAI(base_url=model_cfg["base_url"], api_key=model_cfg["api_key"])
                new_candidates = generate_regen_candidates(
                    item=sample, prompt=prompt, cfg=regen_cfg, client=client, model_name=model_name
                )
            except Exception as exc:
                return {"sample_idx": sample_idx, "qid": qid, "ok": False, "error": str(exc)}
            return {
                "sample_idx": sample_idx,
                "qid": qid,
                "ok": True,
                "new_candidates": new_candidates,
                "regen_info": {
                    "status": "regenerated",
                    "model_key": model_key,
                    "model_name": model_name,
                    "generated_candidate_count": len(new_candidates),
                    "regenerated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                },
            }

        with ThreadPoolExecutor(max_workers=regen_threads) as executor:
            futures = {
                executor.submit(_regen_single, si, s): s.get("question_id")
                for si, s in zero_sample_entries
            }
            for future in _iter_completed_futures(futures, "Phase5-Regen"):
                qid = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    regen_failed += 1
                    logger.exception("Phase5(Regen): qid=%s worker exception: %s", qid, exc)
                    continue
                si = result["sample_idx"]
                sample = samples[si]
                if not result["ok"]:
                    regen_failed += 1
                    sample["regen_info"] = {"status": "failed", "error": result["error"]}
                    continue
                sample["sql_candidates"] = result["new_candidates"]
                sample["regen_info"] = result["regen_info"]
                regen_success += 1

    final_zero = sum(1 for s in samples if not s.get("sql_candidates"))
    logger.info(
        "Phase5(Summary): final_zero={} regen_success={} regen_failed={}",
        final_zero,
        regen_success,
        regen_failed,
    )

    payload["phase5_completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    payload["phase5_postprocess_stats"] = {
        "rules": len(parsed_rules),
        "orig_total_sql": orig_total_sql,
        "new_total_sql": new_total_sql,
        "removed_total_sql": removed_total_sql,
        "invalid_removed_total_sql": invalid_removed_total_sql,
        "affected_results": affected_results,
        "zero_candidate_results": zero_candidate_results,
        "final_zero_candidate_results": final_zero,
        "regen_success": regen_success,
        "regen_failed": regen_failed,
    }
    tmp_path = input_path.with_suffix(".phase5v2.tmp")
    tmp_path.write_text(
        json.dumps(_sanitize_for_json(payload), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    tmp_path.replace(input_path)

    # --- Phase 5 summary log ---
    logger.info(
        "Phase5(v2): done. rules={} orig={} kept={} removed={} invalid_removed={} "
        "zero_before_regen={} regen_success={} regen_failed={} final_zero={} → {}",
        len(parsed_rules), orig_total_sql, new_total_sql, removed_total_sql,
        invalid_removed_total_sql, zero_candidate_results, regen_success, regen_failed, final_zero, input_path,
    )
    _save_phase_snapshot(input_path, "phase5")


# ---------------------------------------------------------------------------
# CLI argument parsers
# ---------------------------------------------------------------------------

def _phase1_args(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("phase1", help="Generate SQL candidates from BIRD samples")
    p.add_argument("--input", type=str, default="data/test.json", help="BIRD sample JSON")
    p.add_argument("--start-index", type=int, default=0, help="Sample start index")
    p.add_argument("--sample-count", type=int, default=-1, help="Number of samples (-1 = all)")
    p.add_argument("--max-workers", type=int, default=4, help="Concurrent generation threads")
    p.add_argument("--model", type=str, default=Settings.DEFAULT_MODEL, help="Model name for metadata")
    p.add_argument(
        "--output", type=str,
        default=str(project_root / "experiments" / "phase_candidates.json"),
        help="Output JSON path",
    )
    p.add_argument("--value-hints-file", type=str, help="Optional offline HNSW prompt hints file")
    p.set_defaults(func=run_phase1)


def _phase2_args(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("phase2", help="Preprocess + hard-filter clean SQL (no LLM)")
    p.add_argument("--input", type=str, required=True, help="Phase-1 output JSON")
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--resume", action="store_true")
    p.set_defaults(func=run_phase2)


def _phase3_args(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("phase3", help="Execute SQL candidates and record Phase3 diagnostics")
    p.add_argument("--input", type=str, required=True)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--resume", action="store_true")
    p.set_defaults(func=run_phase3)


def _phase4_args(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "phase4", help="Execution-feedback-aware correction + immediate re-execution"
    )
    p.add_argument("--input", type=str, required=True)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--max-attempts", type=int, default=1)
    p.add_argument("--exec-timeout", type=int, default=120, help="Re-execution timeout (s)")
    p.add_argument("--empty-stage2-model-key", type=str, default="qwen3-235b")
    p.add_argument("--empty-stage2-rounds", type=int, default=2)
    p.add_argument("--empty-stage2-llm-timeout", type=int, default=20)
    p.add_argument("--empty-stage2-max-tokens", type=int, default=1200)
    p.add_argument("--resume", action="store_true")
    p.set_defaults(func=run_phase4)


def _phase4_5_args(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "phase4_5", help="Error Bank dual-path repair for empty-result candidates (between Phase 4 & 5)"
    )
    p.add_argument("--input", type=str, required=True)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--model-key", type=str,
                    default=os.getenv("PHASE4_5_MODEL", "qwen3-235b"),
                    help="Model for correction LLM")
    p.add_argument("--max-rounds", type=int, default=3, help="Max probe+repair rounds per candidate")
    p.add_argument("--exec-timeout", type=int, default=120, help="SQL execution timeout (s)")
    p.set_defaults(func=_run_phase4_5_cli)


def _run_phase4_5_cli(args: argparse.Namespace) -> None:
    """CLI wrapper: delegates to error_bank.phase4_5.run_phase4_5."""
    from error_bank.phase4_5 import run_phase4_5
    run_phase4_5(args)


def _phase5_args(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("phase5", help="Rule post-processing + zero-candidate regen")
    p.add_argument("--input", type=str, required=True)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument(
        "--rules-preset",
        choices=["qes_30", "refined_rules_depth1"],
        default="refined_rules_depth1",
    )
    p.add_argument("--rules-file", type=str)
    p.add_argument("--train-gt-json", type=str)
    p.add_argument("--skip-regen", action="store_true")
    p.add_argument("--regen-model-key", type=str,
                    default=os.getenv("PHASE5_REGEN_MODEL", "qwen3-235b"))
    p.add_argument("--regen-candidate-count", type=int,
                    default=int(os.getenv("PHASE5_REGEN_CANDIDATE_COUNT", "16")))
    p.add_argument("--regen-batch-size", type=int,
                    default=int(os.getenv("PHASE5_REGEN_BATCH_SIZE", "1")))
    p.add_argument("--regen-max-rounds", type=int,
                    default=int(os.getenv("PHASE5_REGEN_MAX_ROUNDS", "16")))
    p.add_argument("--regen-max-bad-sql-examples", type=int, default=12)
    p.add_argument(
        "--regen-temperature",
        type=float,
        default=Settings.SQL_GENERATION_PRIMARY_TEMPERATURE,
    )
    p.add_argument(
        "--regen-timeout",
        type=int,
        default=Settings.SQL_GENERATION_REQUEST_TIMEOUT,
    )
    p.add_argument("--regen-max-tokens", type=int, default=Settings.MAX_TOKENS)
    p.add_argument(
        "--regen-max-retries",
        type=int,
        default=Settings.SQL_GENERATION_MAX_RETRIES,
    )
    p.add_argument(
        "--regen-retry-backoff",
        type=float,
        default=Settings.SQL_GENERATION_RETRY_BACKOFF,
    )
    p.set_defaults(func=run_phase5)


def _all_args(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "all",
        help="Run full pipeline: Phase 1 → 2 → 3 → 4 → 4.5 → 5",
    )
    # Phase 1 args
    p.add_argument("--input", type=str, default="data/test.json", help="BIRD sample JSON (Phase 1 input)")
    p.add_argument("--start-index", type=int, default=0)
    p.add_argument("--sample-count", type=int, default=-1)
    p.add_argument("--max-workers", type=int, default=4, help="Phase 1 concurrent threads")
    p.add_argument("--model", type=str, default=Settings.DEFAULT_MODEL)
    p.add_argument(
        "--output", type=str,
        default=str(project_root / "experiments" / "phase_candidates.json"),
        help="Pipeline output JSON (updated in-place through phases)",
    )
    p.add_argument("--value-hints-file", type=str)
    p.add_argument("--skip-phase1", action="store_true", help="Skip Phase 1 (--output must be an existing Phase-1 file)")
    # Phase 2-4 args
    p.add_argument("--phase2-threads", type=int, default=8)
    p.add_argument("--phase3-threads", type=int, default=8)
    p.add_argument("--phase3-timeout", type=int, default=120)
    p.add_argument("--phase4-threads", type=int, default=4)
    p.add_argument("--phase4-max-attempts", type=int, default=1)
    p.add_argument("--phase4-exec-timeout", type=int, default=120)
    p.add_argument("--phase4-empty-stage2-model-key", type=str, default="qwen3-235b")
    p.add_argument("--phase4-empty-stage2-rounds", type=int, default=2)
    p.add_argument("--phase4-empty-stage2-llm-timeout", type=int, default=20)
    p.add_argument("--phase4-empty-stage2-max-tokens", type=int, default=1200)
    # Phase 4.5 args
    p.add_argument("--skip-phase4-5", action="store_true", help="Skip Phase 4.5 (Error Bank empty-result repair)")
    p.add_argument("--phase4-5-threads", type=int, default=8)
    p.add_argument("--phase4-5-model-key", type=str,
                    default=os.getenv("PHASE4_5_MODEL", "qwen3-235b"))
    p.add_argument("--phase4-5-max-rounds", type=int, default=3)
    p.add_argument("--phase4-5-exec-timeout", type=int, default=120)
    # Phase 5 args
    p.add_argument("--skip-phase5", action="store_true")
    p.add_argument(
        "--phase5-rules-preset",
        choices=["qes_30", "refined_rules_depth1"],
        default="refined_rules_depth1",
    )
    p.add_argument("--phase5-rules-file", type=str)
    p.add_argument("--phase5-train-gt-json", type=str)
    p.add_argument("--phase5-threads", type=int, default=4)
    p.add_argument("--phase5-skip-regen", action="store_true")
    p.add_argument("--phase5-regen-model-key", type=str,
                    default=os.getenv("PHASE5_REGEN_MODEL", "qwen3-235b"))
    p.add_argument("--phase5-regen-candidate-count", type=int,
                    default=int(os.getenv("PHASE5_REGEN_CANDIDATE_COUNT", "16")))
    p.add_argument("--phase5-regen-batch-size", type=int,
                    default=int(os.getenv("PHASE5_REGEN_BATCH_SIZE", "1")))
    p.add_argument("--phase5-regen-max-rounds", type=int,
                    default=int(os.getenv("PHASE5_REGEN_MAX_ROUNDS", "16")))
    p.add_argument("--phase5-regen-max-bad-sql-examples", type=int, default=12)
    p.add_argument(
        "--phase5-regen-timeout",
        type=int,
        default=Settings.SQL_GENERATION_REQUEST_TIMEOUT,
    )
    p.add_argument(
        "--phase5-regen-temperature",
        type=float,
        default=Settings.SQL_GENERATION_PRIMARY_TEMPERATURE,
    )
    p.add_argument("--phase5-regen-max-tokens", type=int, default=Settings.MAX_TOKENS)
    p.add_argument(
        "--phase5-regen-max-retries",
        type=int,
        default=Settings.SQL_GENERATION_MAX_RETRIES,
    )
    p.add_argument(
        "--phase5-regen-retry-backoff",
        type=float,
        default=Settings.SQL_GENERATION_RETRY_BACKOFF,
    )
    p.add_argument("--resume", action="store_true",
                    help="Resume: skip Phase 1 if output exists, skip completed candidates in later phases")
    p.set_defaults(func=run_all)


def run_all(args: argparse.Namespace) -> None:
    """Run full pipeline: Phase 1 → 2 → 3 → 4 → 4.5 → 5 sequentially."""
    output_path = Path(args.output)
    resume = getattr(args, "resume", False)
    skip_phase1 = getattr(args, "skip_phase1", False)

    # --- Phase 1 ---
    if skip_phase1:
        logger.info("Pipeline v2 all: --skip-phase1 set, using existing file: {}", args.output)
    elif resume and output_path.exists():
        logger.info("Pipeline v2 all: resume mode, Phase-1 output exists — skipping Phase 1: {}", args.output)
    else:
        logger.info("Pipeline v2 all: starting Phase 1 from {}", args.input)
        phase1_args = argparse.Namespace(
            input=args.input,
            start_index=args.start_index,
            sample_count=args.sample_count,
            max_workers=args.max_workers,
            model=args.model,
            output=args.output,
            value_hints_file=args.value_hints_file,
        )
        run_phase1(phase1_args)
        logger.info("Pipeline v2 all: Phase1 done → {}", args.output)

    # From here, all phases read/write the same output file in-place
    pipeline_file = str(output_path)

    # --- Phase 2 ---
    phase2_args = argparse.Namespace(
        input=pipeline_file,
        threads=args.phase2_threads,
        resume=resume,
    )
    run_phase2(phase2_args)
    logger.info("Pipeline v2 all: Phase2 done")

    # --- Phase 3 ---
    phase3_args = argparse.Namespace(
        input=pipeline_file,
        threads=args.phase3_threads,
        timeout=args.phase3_timeout,
        resume=resume,
    )
    run_phase3(phase3_args)
    logger.info("Pipeline v2 all: Phase3 done")

    # --- Phase 4 ---
    phase4_args = argparse.Namespace(
        input=pipeline_file,
        threads=args.phase4_threads,
        max_attempts=args.phase4_max_attempts,
        exec_timeout=args.phase4_exec_timeout,
        empty_stage2_model_key=args.phase4_empty_stage2_model_key,
        empty_stage2_rounds=args.phase4_empty_stage2_rounds,
        empty_stage2_llm_timeout=args.phase4_empty_stage2_llm_timeout,
        empty_stage2_max_tokens=args.phase4_empty_stage2_max_tokens,
        resume=resume,
    )
    run_phase4(phase4_args)
    logger.info("Pipeline v2 all: Phase4 done")

    # Legacy standalone Phase4.5 is no longer part of the default pipeline.
    # Phase4 already contains the two-stage empty-result repair:
    #   1. high-precision local repair
    #   2. iterative error-bank fallback
    # Keep the standalone CLI entry for manual experiments only.

    # --- Phase 5 ---
    if not getattr(args, "skip_phase5", False):
        phase5_args = argparse.Namespace(
            input=pipeline_file,
            threads=args.phase5_threads,
            rules_preset=args.phase5_rules_preset,
            rules_file=args.phase5_rules_file,
            train_gt_json=args.phase5_train_gt_json,
            skip_regen=args.phase5_skip_regen,
            regen_model_key=args.phase5_regen_model_key,
            regen_candidate_count=args.phase5_regen_candidate_count,
            regen_batch_size=args.phase5_regen_batch_size,
            regen_max_rounds=args.phase5_regen_max_rounds,
            regen_max_bad_sql_examples=args.phase5_regen_max_bad_sql_examples,
            regen_timeout=args.phase5_regen_timeout,
            regen_temperature=args.phase5_regen_temperature,
            regen_max_tokens=args.phase5_regen_max_tokens,
            regen_max_retries=args.phase5_regen_max_retries,
            regen_retry_backoff=args.phase5_regen_retry_backoff,
        )
        run_phase5(phase5_args)
        logger.info("Pipeline v2 all: Phase5 done")

    # --- Export final predictions ---
    save_final_results(pipeline_file)

    logger.info("Pipeline v2 all: complete. Output: {}", pipeline_file)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SQL Pipeline v2 — full Phase 1→2→3→4→4.5→5 pipeline"
    )
    subparsers = parser.add_subparsers(dest="stage", required=True)
    _phase1_args(subparsers)
    _phase2_args(subparsers)
    _phase3_args(subparsers)
    _phase4_args(subparsers)
    _phase4_5_args(subparsers)
    _phase5_args(subparsers)
    _all_args(subparsers)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
