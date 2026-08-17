"""Phase 4.5: Error Bank powered dual-path empty-result repair.

Inserts between Phase 4 and Phase 5 in the pipeline.
For each still-empty candidate after Phase 4:
  1. Run progressive probing to diagnose root cause
  2. Record diagnosis in error bank
  3. Dual-path repair: baseline (rewrite) + probe (targeted fix)
  4. Execute both, keep non-empty results as new candidates
  5. Multi-round: if still empty, repeat with accumulated history

New candidates are APPENDED to the candidate pool (not replacing originals).
This enriches the pool for downstream voting/selection.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tqdm
from loguru import logger
from openai import OpenAI

from error_bank.diagnosis_spec import build_structured_empty_sql_diagnosis
from error_bank.store import ErrorBankStore
from error_bank.collector import collect_probe_diagnosis
from error_bank.prober import diagnose_empty_result
from error_bank.retriever import retrieve as bank_retrieve

# Lazy imports for enhanced probing (avoid circular)
_enhanced_imports = {}


def _get_enhanced():
    if not _enhanced_imports:
        from scripts.test_enhanced_probe import (
            cross_table_search, combination_probe, enhanced_where_probe,
            function_filter_probe, _resolve_alias,
        )
        _enhanced_imports.update({
            'cross_table_search': cross_table_search,
            'combination_probe': combination_probe,
            'enhanced_where_probe': enhanced_where_probe,
            'function_filter_probe': function_filter_probe,
            '_resolve_alias': _resolve_alias,
        })
    return _enhanced_imports


def _stderr_is_tty() -> bool:
    isatty = getattr(sys.stderr, "isatty", None)
    if not callable(isatty):
        return False
    try:
        return bool(isatty())
    except Exception:
        return False


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


def _run_jobs_with_deadline(
    jobs: List[Dict[str, Any]],
    worker_fn,
    desc: str,
    threads: int,
    worker_timeout: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Run a batch of jobs with bounded waiting and periodic progress logs."""
    if not jobs:
        return [], []

    executor = ThreadPoolExecutor(max_workers=threads)
    futures = {executor.submit(worker_fn, job): job for job in jobs}
    pending_futures = set(futures.keys())
    deadline = time.time() + worker_timeout + 60

    log_every = max(1, (len(futures) + 19) // 20)
    done_count = 0
    last_log = time.monotonic()
    results: List[Dict[str, Any]] = []

    logger.info("{}: start total={}", desc, len(futures))
    while pending_futures and time.time() < deadline:
        newly_done, pending_futures = wait(
            pending_futures,
            timeout=30,
            return_when=FIRST_COMPLETED,
        )

        for future in newly_done:
            done_count += 1
            if done_count % log_every == 0 or done_count == len(futures):
                logger.info("{}: progress {}/{}", desc, done_count, len(futures))
            try:
                results.append(future.result(timeout=0))
            except Exception as exc:
                job = futures[future]
                logger.warning("{}: worker error on job {}: {}", desc, job.get("job_idx"), exc)

        if not newly_done and time.monotonic() - last_log > 120:
            logger.warning(
                "{}: {} workers still pending, deadline in {:.0f}s",
                desc,
                len(pending_futures),
                deadline - time.time(),
            )
            last_log = time.monotonic()

    timed_out_jobs = [futures[future] for future in pending_futures]
    if timed_out_jobs:
        logger.warning(
            "{}: deadline reached, abandoning {} jobs (completed {}/{})",
            desc,
            len(timed_out_jobs),
            done_count,
            len(futures),
        )
    executor.shutdown(wait=False, cancel_futures=True)
    return results, timed_out_jobs


# ── SQL execution (matches pipeline's _exec_sql signature) ────

def _exec_sql(sql_text: str, db_path: str, timeout: int = 120) -> Dict[str, Any]:
    sql_text = sql_text.strip()
    if not sql_text:
        return {"status": "skipped", "rows": None, "error": "empty_sql", "elapsed": 0.0, "empty_result": True}
    conn = sqlite3.connect(db_path)
    start = time.perf_counter()

    def _progress():
        if timeout > 0 and time.perf_counter() - start > timeout:
            raise TimeoutError("timeout")

    try:
        if timeout > 0:
            conn.set_progress_handler(_progress, 1000)
        cur = conn.cursor()
        cur.execute(sql_text)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - start
        return {
            "status": "succeeded", "rows": rows, "error": None,
            "elapsed": elapsed, "empty_result": len(rows) == 0,
        }
    except TimeoutError:
        return {"status": "timeout", "rows": None, "error": "timeout",
                "elapsed": time.perf_counter() - start, "empty_result": True}
    except Exception as e:
        return {"status": "failed", "rows": None, "error": str(e)[:200],
                "elapsed": time.perf_counter() - start, "empty_result": True}
    finally:
        conn.set_progress_handler(None, 0)
        conn.close()


def _extract_sql(content: str) -> str:
    m = re.search(r'```sql\s*\n?(.*?)```', content, re.S | re.I)
    if m and m.group(1).strip().upper().startswith(('SELECT', 'WITH')):
        return m.group(1).strip()
    m = re.search(r'```\s*\n?((?:SELECT|WITH).*?)```', content, re.S | re.I)
    if m:
        return m.group(1).strip()
    lines, out = content.strip().split('\n'), []
    for l in lines:
        s = l.strip()
        if s.upper().startswith(('SELECT ', 'WITH ')): out = [s]
        elif out and s and not s.startswith(('Let','The','This','Note','1.','2.','-','*')): out.append(s)
    return '\n'.join(out).rstrip(';').strip()


def _extract_tables(sql: str) -> List[str]:
    tables = set()
    for m in re.finditer(r'\bFROM\s+(\w+)', sql, re.I): tables.add(m.group(1))
    for m in re.finditer(r'\bJOIN\s+(\w+)', sql, re.I): tables.add(m.group(1))
    return sorted(tables)


def _build_literal_guardrails(question: str, evidence: str, sql_text: str) -> str:
    """Collect user-facing literals that the repair should avoid silently changing."""
    protected: List[str] = []
    seen = set()

    def _add(value: str, source: str) -> None:
        norm = value.strip()
        if not norm:
            return
        key = (source, norm)
        if key in seen:
            return
        seen.add(key)
        protected.append(f"- {source}: {norm}")

    for source, text in [
        ("question", question or ""),
        ("evidence", evidence or ""),
    ]:
        for value in re.findall(r"""['"]([^'"]{1,120})['"]""", text):
            _add(value, source)
        for value in re.findall(r"""\b\d{1,4}(?:[-/]\d{1,2}[-/]\d{1,4})?\b""", text):
            _add(value, source)

    for value in re.findall(r"""'([^']{1,120})'""", sql_text):
        _add(value, "sql")
    for value in re.findall(r'''"([^"]{1,120})"''', sql_text):
        _add(value, "sql")
    for value in re.findall(r"""\b\d{1,4}(?:[-/]\d{1,2}[-/]\d{1,4})?\b""", sql_text):
        _add(value, "sql")

    if not protected:
        return ""

    return (
        "PROTECTED LITERALS:\n"
        "Do NOT change these question/entity/value literals unless the probing section explicitly provides "
        "an exact DB-backed replacement.\n"
        + "\n".join(protected[:16])
    )


def _build_probe_repair_rules(diag, has_cross_table_hit: bool) -> str:
    """Build diagnosis-aware rules so the probe path patches the SQL instead of drifting semantically."""
    lines = [
        "CRITICAL REPAIR RULES:",
        "1. Answer the original question exactly. A non-empty but semantically different query is a failure.",
        "2. Make the smallest possible patch to the failing SQL. Keep unaffected SELECT/JOIN/filters unchanged.",
        "3. Do NOT silently replace titles, names, IDs, dates, thresholds, or quoted strings unless the probing section gives an exact DB-backed replacement.",
        "4. If the diagnosis only identifies one killer clause, modify that clause first instead of rewriting the whole query.",
    ]

    root_cause = getattr(diag, "root_cause", "") if diag else ""
    if has_cross_table_hit:
        lines.append(
            "5. The value exists in a different table. Move the predicate to the correct table/join path; keep the value itself unchanged."
        )
    elif root_cause in {"FUZZY_MISMATCH", "CASE_MISMATCH", "PREFIX_MATCH", "SUBSTRING_MATCH"}:
        lines.append(
            "5. This is a value mismatch. Prefer the exact DB-backed closest match from the probe; do not invent a new value."
        )
    elif root_cause == "VALUE_NOT_EXISTS":
        lines.append(
            "5. The current value/table combination is unsupported by the DB. Keep the original value unless the probe gives an exact replacement; otherwise change the table/column/join."
        )
    elif root_cause in {"HAVING_TOO_RESTRICTIVE", "COMPLEX_WHERE_KILLER"}:
        lines.append(
            "5. The filter logic is too restrictive. Relax or remove only the diagnosed killer predicate; do not broaden unrelated joins or switch the task target."
        )
    elif root_cause in {"FUNCTION_FILTER_KILLS", "SUBQUERY_VALUE_MISMATCH", "SUBQUERY_RETURNS_EMPTY"}:
        lines.append(
            "5. The function/subquery is the issue. Rewrite that specific function/subquery while preserving the rest of the question semantics."
        )

    if diag:
        value_probe_lines = []
        for probe in getattr(diag, "value_probes", [])[:4]:
            pieces = [f"{probe.column} expected='{probe.expected_value}'"]
            if probe.closest_match:
                pieces.append(f"closest_match='{probe.closest_match}'")
            if probe.suggested_fix:
                pieces.append(f"suggested_fix='{probe.suggested_fix}'")
            value_probe_lines.append("- " + ", ".join(pieces))
        if value_probe_lines:
            lines.append("DB-BACKED VALUE HINTS:")
            lines.extend(value_probe_lines)

    lines.append("Return one corrected SQL only.")
    return "\n".join(lines)


def _extract_protected_literals(question: str, evidence: str, sql_text: str) -> set[str]:
    literals: set[str] = set()

    def _collect(text: str) -> None:
        for value in re.findall(r"""['"]([^'"]{1,120})['"]""", text):
            if value.strip():
                literals.add(value.strip())
        for value in re.findall(r"""\b\d{1,4}(?:[-/]\d{1,2}[-/]\d{1,4})?\b""", text):
            if value.strip():
                literals.add(value.strip())

    _collect(question or "")
    _collect(evidence or "")
    _collect(sql_text or "")
    return literals


def _extract_allowed_probe_literals(diag) -> set[str]:
    allowed: set[str] = set()
    if not diag:
        return allowed

    def _add(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _add(item)
            return
        text = str(value).strip()
        if text:
            allowed.add(text)

    _add(getattr(diag, "root_cause_detail", ""))
    _add(getattr(diag, "fix_description", ""))
    for probe in getattr(diag, "value_probes", []) or []:
        _add(getattr(probe, "expected_value", None))
        _add(getattr(probe, "closest_match", None))
        _add(getattr(probe, "suggested_fix", None))
        _add(getattr(probe, "actual_distinct_values", None))
    return allowed


def _extract_sql_literals(sql_text: str) -> set[str]:
    literals: set[str] = set()
    for value in re.findall(r"""'([^']{1,120})'""", sql_text or ""):
        if value.strip():
            literals.add(value.strip())
    for value in re.findall(r'''"([^"]{1,120})"''', sql_text or ""):
        if value.strip():
            literals.add(value.strip())
    for value in re.findall(r"""\b\d{2,4}(?:[-/]\d{1,2}[-/]\d{1,4})?\b""", sql_text or ""):
        if value.strip():
            literals.add(value.strip())
    return literals


def _literal_matches_allowed(literal: str, allowed_literals: set[str]) -> bool:
    for allowed in allowed_literals:
        if literal == allowed:
            return True
        if len(literal) >= 4 and (literal in allowed or allowed in literal):
            return True
    return False


def _probe_literal_drift_detected(
    *,
    question: str,
    evidence: str,
    current_sql: str,
    new_sql: str,
    diag,
) -> bool:
    source_literals = _extract_protected_literals(question, evidence, current_sql)
    allowed_literals = set(source_literals)
    allowed_literals.update(_extract_allowed_probe_literals(diag))
    for literal in _extract_sql_literals(new_sql):
        if not _literal_matches_allowed(literal, allowed_literals):
            return True
    return False


def _extract_having_thresholds(sql_text: str) -> set[str]:
    thresholds: set[str] = set()
    having_match = re.search(r'\bHAVING\b\s+(.+?)(?:\bORDER\b|\bLIMIT\b|$)', sql_text or "", re.I | re.S)
    if not having_match:
        return thresholds
    having_text = having_match.group(1)
    for value in re.findall(r"""(?:>=|<=|=|>|<)\s*(\d+(?:\.\d+)?)""", having_text):
        thresholds.add(value)
    return thresholds


def _having_threshold_drift_detected(current_sql: str, new_sql: str, question: str, evidence: str) -> bool:
    original_thresholds = _extract_having_thresholds(current_sql)
    if not original_thresholds:
        return False
    new_thresholds = _extract_having_thresholds(new_sql)
    if not new_thresholds or new_thresholds == original_thresholds:
        return False
    allowed = _extract_protected_literals(question, evidence, current_sql)
    return any(threshold not in allowed for threshold in new_thresholds)


def _select_repair_paths(root_cause: str) -> list[tuple[str, str, str]]:
    skip_all = {
        "HAVING_TOO_RESTRICTIVE",
        "PROBE_FAILED",
        "NOT_ACTUALLY_EMPTY",
        "FUNCTION_FILTER_KILLS",
    }
    probe_enabled = {
        "FUZZY_MISMATCH",
        "CASE_MISMATCH",
        "PREFIX_MATCH",
        "SUBSTRING_MATCH",
        "VALUE_NOT_EXISTS",
        "EXACT_MATCH_EXISTS",
        "COMPLEX_WHERE_KILLER",
        "SUBQUERY_VALUE_MISMATCH",
    }

    if root_cause in skip_all:
        return []

    paths = [("baseline", _SYS_BASELINE, _TPL_BASELINE)]
    if root_cause in probe_enabled:
        paths.append(("probe", _SYS_PROBE, _TPL_PROBE))
    return paths


def _exec_count_safe(sql_text: str, db_path: str) -> int:
    try:
        import sqlite3
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
        r = conn.execute(sql_text).fetchone()
        conn.close()
        return int(r[0]) if r else 0
    except: return -1


def _exec_values_safe(sql_text: str, db_path: str):
    try:
        import sqlite3
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
        rows = conn.execute(sql_text).fetchall()
        conn.close()
        return [row[0] for row in rows]
    except: return []


# ── Probe diagnosis formatting (with enhanced analysis) ──────

def _format_probe_diagnosis(diag, sql: str, db_path: str) -> tuple:
    enh = _get_enhanced()
    lines = []
    for s in diag.stages:
        mk = " ◀◀◀ KILLER" if s.is_killer else ""
        c = f"{s.row_count:>8}" if s.row_count >= 0 else "   ERROR"
        d = f" (dropped {s.drop_from_prev})" if s.drop_from_prev and s.drop_from_prev > 0 else ""
        lines.append(f"  Stage {s.stage_idx:>2} [{s.stage_type:>7}]: {c} rows{d}  | {s.condition_text[:90]}{mk}")
    t = "Filter Chain:\n" + "\n".join(lines)
    t += f"\n\nRoot Cause: {diag.root_cause}\nDetail: {diag.root_cause_detail[:300]}"
    for vp in diag.value_probes:
        t += f"\n\nValue Probe [{vp.column}]: expected='{vp.expected_value}', actual={vp.actual_distinct_values[:8]}"
        if vp.closest_match: t += f", closest='{vp.closest_match}'"
        if vp.suggested_fix: t += f", fix={vp.suggested_fix}"
    if diag.fix_description: t += f"\n\nFix: {diag.fix_description}"
    # Enhanced probing
    rc = diag.root_cause
    sql_c = sql.rstrip().rstrip(';')
    fm = re.search(r'\bFROM\b(.+?)(?:\bWHERE\b|\bGROUP|\bORDER|\bLIMIT|$)', sql_c, re.I | re.S)
    fc = f"FROM{fm.group(1)}" if fm else ""
    has_cross_table_hit = False
    try:
        if rc == "VALUE_NOT_EXISTS":
            m = re.search(r"\[(\w+\.?\w*)\].*?expected '([^']*)'", diag.root_cause_detail)
            if not m: m = re.search(r"\[(\w+)\]\s*=\s*'([^']*)'", diag.root_cause_detail)
            if m:
                cn = m.group(1).split('.')[-1]
                ct = enh['_resolve_alias'](m.group(1).split('.')[0], sql) if '.' in m.group(1) else ""
                s = enh['cross_table_search'](cn, m.group(2), ct, sql, db_path)
                t += f"\n\nCross-Table:\n{s}"
                if "FOUND in:" in s:
                    has_cross_table_hit = True
                    t += f"\n\n*** ACTION REQUIRED: The value exists in a DIFFERENT table. You MUST rewrite the query to use that table instead. ***"
        elif rc == "EXACT_MATCH_EXISTS":
            cb = enh['combination_probe'](sql_c, db_path)
            if cb: t += f"\n\n{cb}"
        elif rc == "COMPLEX_WHERE_KILLER":
            m = re.search(r"Cannot parse condition:\s*(.+)", diag.root_cause_detail, re.S)
            if m:
                e = enh['enhanced_where_probe'](m.group(1).strip()[:200], fc, sql_c, db_path)
                if e: t += f"\n\nEnhanced:\n{e}"
            # Also run combination analysis — the killer may not be the only bad condition
            cb = enh['combination_probe'](sql_c, db_path)
            if cb: t += f"\n\n{cb}"
        elif rc == "FUNCTION_FILTER_KILLS":
            m = re.search(r"Complex function condition kills all rows:\s*(.+)", diag.root_cause_detail, re.S)
            if m:
                e = enh['function_filter_probe'](m.group(1).strip()[:300], fc, sql_c, db_path)
                if e: t += f"\n\nFunc:\n{e}"

        # Improvement 3: For any root cause, if killer is a WHERE condition,
        # also check values in the JOIN context (not just raw table)
        if diag.killer_stage and diag.killer_stage.stage_type == "where" and fc:
            killer_cond = diag.killer_stage.condition_text
            for prefix in ["[!SOLO_KILL] ", "[L] ", "[R] "]:
                killer_cond = killer_cond.replace(prefix, "")
            # Execute the killer condition in the JOIN context
            ctx_count = _exec_count_safe(f"SELECT COUNT(*) {fc} WHERE {killer_cond}", db_path)
            if ctx_count == 0:
                # Check what values exist AFTER the JOIN
                eq_m = re.search(r"""([\w.`"]+)\s+IN\s*\(([^)]+)\)""", killer_cond, re.I)
                if not eq_m:
                    eq_m = re.search(r"""([\w.`"]+)\s*=\s*'([^']*)'""", killer_cond)
                if eq_m:
                    col_expr = eq_m.group(1)
                    # Get actual values in JOIN context
                    ctx_vals = _exec_values_safe(
                        f"SELECT DISTINCT {col_expr} {fc} WHERE {col_expr} IS NOT NULL LIMIT 15", db_path)
                    if ctx_vals:
                        t += f"\n\nJOIN-context values for {col_expr}: {[str(v) for v in ctx_vals[:10]]}"
                        t += f"\n(These are the actual values available AFTER the JOIN, not in the raw table)"
    except: pass
    return t, has_cross_table_hit


# ── LLM prompts ──────────────────────────────────────────────

_SYS_BASELINE = (
    "You are an expert SQLite SQL writer. "
    "Write a completely new SQL query from scratch. "
    "Output ONLY SQL in a ```sql``` block."
)

_SYS_PROBE = (
    "You are an expert SQLite SQL debugger. "
    "Apply a targeted fix based on the probing results. "
    "Output ONLY corrected SQL in a ```sql``` block."
)

_TPL_BASELINE = """Database Engine: SQLite
Question: {question}
Evidence: {evidence}
Schema:
{schema}
{db_facts}
{history}

A previous SQL returned 0 rows (for reference only):
--- failed SQL ---
{sql}
--- end ---

Write a COMPLETELY NEW SQL from scratch.
Do NOT modify the failed SQL. Approach fresh.
Output ONLY SQL in a ```sql``` block."""

_TPL_PROBE = """Database Engine: SQLite
Question: {question}
Evidence: {evidence}
Schema:
{schema}
{db_facts}
{history}
{literal_guardrails}

SQL to fix (returns 0 rows):
{sql}

═══════════════════════════════════════════════
DATABASE PROBING:
═══════════════════════════════════════════════
{diagnosis}
═══════════════════════════════════════════════

{probe_instruction}
{repair_rules}
Output ONLY SQL in a ```sql``` block."""


# ── Core: diagnose first, then repair by round ───────────────

def _build_bank_facts_section(
    bank: ErrorBankStore,
    qid: int,
    db_id: str,
    sql_text: str,
    diag,
    question: str,
) -> str:
    columns = diag.schema_anchors if diag else []
    tables = _extract_tables(sql_text)
    lines: List[str] = []
    for hit in bank_retrieve(bank, qid, db_id, tables, columns, question):
        entry = hit.entry
        if entry.question_id == qid:
            continue
        if entry.error_phase != "phase4.5_probe":
            continue
        if not entry.db_facts:
            continue
        for fact in entry.db_facts:
            prefix = "Verified" if entry.fix_succeeded else "Observed"
            lines.append(f"{prefix}: {fact.column} actual={fact.actual_values[:5]}")
            if fact.suggested_fix:
                lines.append(f"  fix={fact.suggested_fix}")
        if len(lines) >= 12:
            break
    return f"\nKnown DB facts:\n" + "\n".join(lines) if lines else ""


def _diagnose_one_candidate(
    state: Dict[str, Any],
    bank: ErrorBankStore,
    exec_timeout: int,
) -> Dict[str, Any]:
    sample = state["sample"]
    qid = sample["question_id"]
    db_id = sample["db_id"]
    current_sql = state.get("current_sql", "").strip()
    diag = None
    diag_text = "(probing failed or timed out)"
    has_cross_table_hit = False
    bank_entry = None
    structured_diagnosis = None

    if current_sql:
        try:
            import concurrent.futures as _cf
            probe_exec = _cf.ThreadPoolExecutor(max_workers=1)
            probe_future = probe_exec.submit(
                diagnose_empty_result,
                qid,
                db_id,
                current_sql,
                state["db_path"],
                exec_timeout,
            )
            try:
                diag = probe_future.result(timeout=exec_timeout)
            except _cf.TimeoutError:
                try:
                    probe_future.cancel()
                except Exception:
                    pass
                try:
                    probe_exec.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    probe_exec.shutdown(wait=False)
                diag = None
            finally:
                try:
                    probe_exec.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    probe_exec.shutdown(wait=False)
            diag_text, has_cross_table_hit = _format_probe_diagnosis(
                diag,
                current_sql,
                state["db_path"],
            )
        except Exception:
            diag = None
            diag_text = "(probing failed or timed out)"
            has_cross_table_hit = False
        try:
            structured_diagnosis = build_structured_empty_sql_diagnosis(
                question_id=qid,
                db_id=db_id,
                question=sample.get("question", ""),
                evidence=sample.get("evidence", ""),
                sql_text=current_sql,
                diagnosis=diag,
                db_path=state["db_path"],
            ).to_dict()
        except Exception:
            structured_diagnosis = None

    if diag:
        try:
            bank_entry = collect_probe_diagnosis(
                qid,
                db_id,
                sample.get("question", ""),
                sample.get("evidence", ""),
                diag,
            )
            bank.insert(bank_entry)
        except Exception:
            bank_entry = None

    return {
        "job_idx": state["job_idx"],
        "diag": diag,
        "diag_text": diag_text,
        "has_cross_table_hit": has_cross_table_hit,
        "bank_entry": bank_entry,
        "structured_diagnosis": structured_diagnosis,
    }


def _repair_one_candidate_round(
    state: Dict[str, Any],
    bank: ErrorBankStore,
    client: OpenAI,
    model_name: str,
    round_idx: int,
    exec_timeout: int,
    llm_timeout: int,
    max_tokens: int,
) -> Dict[str, Any]:
    sample = state["sample"]
    cand_idx = state["cand_idx"]
    current_sql = state.get("current_sql", "").strip()
    qid = sample["question_id"]
    db_id = sample["db_id"]
    question = sample.get("question", "")
    evidence = sample.get("evidence", "")
    schema_info = sample.get("schema_info", {})
    schema = schema_info.get("schema_description") or schema_info.get("schema_prompt_text") or ""
    diag = state.get("diag")
    diag_text = state.get("diag_text") or "(probing failed or timed out)"
    root_cause = getattr(diag, "root_cause", "PROBE_FAILED" if not diag else "unknown")
    has_cross_table_hit = bool(state.get("has_cross_table_hit"))
    history_lines = list(state.get("history_lines") or [])
    structured_diagnosis = state.get("structured_diagnosis") or {}

    db_facts_section = _build_bank_facts_section(
        bank,
        qid,
        db_id,
        current_sql,
        diag,
        question,
    )
    history_text = "\n".join(history_lines) if history_lines else ""

    def _call(sys_prompt, user_prompt):
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=max_tokens,
            timeout=llm_timeout,
        )
        return _extract_sql(resp.choices[0].message.content or "")

    if has_cross_table_hit:
        probe_instruction = (
            "The probing found the value in a DIFFERENT table. "
            "You MUST rewrite the query to use that table. "
            "Do NOT just change the value — change the FROM/JOIN to use the correct table."
        )
    else:
        probe_instruction = "Apply a TARGETED FIX based on probing."

    results = []
    for path, sys_p, tpl in _select_repair_paths(root_cause):
        try:
            prompt_kwargs = dict(
                question=question,
                evidence=evidence or "N/A",
                schema=schema[:3000],
                sql=current_sql,
                db_facts=db_facts_section,
                history=history_text,
                literal_guardrails="",
            )
            if path == "probe":
                prompt_kwargs["diagnosis"] = diag_text
                prompt_kwargs["probe_instruction"] = probe_instruction
                prompt_kwargs["literal_guardrails"] = _build_literal_guardrails(
                    question,
                    evidence,
                    current_sql,
                )
                prompt_kwargs["repair_rules"] = _build_probe_repair_rules(
                    diag,
                    has_cross_table_hit,
                )
            new_sql = _call(sys_p, tpl.format(**prompt_kwargs))
            if not new_sql:
                continue
            if path == "probe":
                if _probe_literal_drift_detected(
                    question=question,
                    evidence=evidence,
                    current_sql=current_sql,
                    new_sql=new_sql,
                    diag=diag,
                ):
                    results.append(
                        (
                            path,
                            new_sql,
                            {
                                "status": "failed",
                                "rows": None,
                                "error": "literal_drift_guard",
                                "elapsed": None,
                                "empty_result": True,
                            },
                        )
                    )
                    continue
                if root_cause == "HAVING_TOO_RESTRICTIVE" and _having_threshold_drift_detected(
                    current_sql,
                    new_sql,
                    question,
                    evidence,
                ):
                    results.append(
                        (
                            path,
                            new_sql,
                            {
                                "status": "failed",
                                "rows": None,
                                "error": "having_threshold_drift_guard",
                                "elapsed": None,
                                "empty_result": True,
                            },
                        )
                    )
                    continue
            ex = _exec_sql(new_sql, state["db_path"], exec_timeout)
            results.append((path, new_sql, ex))
        except Exception:
            continue

    new_candidates = []
    any_nonempty = False
    success_fix_sql = ""
    for path, new_sql, ex in results:
        if ex.get("status") == "succeeded" and not ex.get("empty_result"):
            any_nonempty = True
            if not success_fix_sql:
                success_fix_sql = new_sql
            new_candidates.append({
                "sql": new_sql,
                "source": f"phase4.5_{path}_r{round_idx}",
                "source_group": "phase4.5_error_bank",
                "source_model": model_name,
                "vote_count": 1,
                "result_vote_score": 0,
                "validation": {"status": "autocorrected", "errors": [], "warnings": []},
                "execution": {
                    "status": ex["status"],
                    "rows": ex.get("rows"),
                    "error": ex.get("error"),
                    "elapsed": ex.get("elapsed"),
                    "empty_result": ex.get("empty_result", False),
                },
                "phase4_5_info": {
                    "round": round_idx,
                    "path": path,
                    "root_cause": diag.root_cause if diag else "unknown",
                    "original_cand_idx": cand_idx,
                    "had_bank_facts": bool(db_facts_section),
                    "task_spec": structured_diagnosis.get("task_spec", {}),
                    "failure_spec": structured_diagnosis.get("failure_spec", {}),
                    "patch_plan": structured_diagnosis.get("patch_plan", {}),
                    "alignment_spec": structured_diagnosis.get("alignment_spec", {}),
                },
            })

    history_updates: List[str] = []
    for path, new_sql, ex in results:
        status = "EMPTY" if ex.get("empty_result") else f"{len(ex.get('rows') or [])} rows"
        if ex.get("status") != "succeeded":
            status = f"ERROR: {ex.get('error', '')[:60]}"
        history_updates.append(f"Round {round_idx} ({path}): {status}")
        history_updates.append(f"  SQL: {new_sql[:200]}")

    next_sql = current_sql
    for path, new_sql, _ in results:
        if path == "probe" and new_sql:
            next_sql = new_sql
            break

    return {
        "job_idx": state["job_idx"],
        "new_candidates": new_candidates,
        "history_updates": history_updates,
        "done": any_nonempty,
        "next_sql": next_sql,
        "fix_sql": success_fix_sql,
        "had_bank_facts": bool(db_facts_section),
    }


# ── Phase 4.5 entry point ────────────────────────────────────

def run_phase4_5(args: argparse.Namespace) -> None:
    """Phase 4.5: Error Bank powered dual-path repair for empty-result candidates."""
    from config import Settings
    from config.model_config import get_model_config

    input_path = Path(args.input)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    samples = payload.get("results", [])

    model_key = getattr(args, "model_key", "qwen3-235b")
    model_cfg = get_model_config(model_key)
    max_rounds = getattr(args, "max_rounds", 3)
    exec_timeout = getattr(args, "exec_timeout", 120)
    threads = getattr(args, "threads", 8)

    # Initialize error bank
    bank = ErrorBankStore()

    # Collect jobs: all still-empty candidates
    jobs = []
    for sample in samples:
        db_id = sample.get("db_id")
        try:
            db_path = str(Settings.get_database_path(db_id))
        except FileNotFoundError:
            continue

        for idx, cand in enumerate(sample.get("sql_candidates", [])):
            ex = cand.get("execution", {})
            if ex.get("empty_result") and ex.get("status") == "succeeded":
                jobs.append((sample, cand, idx, db_path))

    if not jobs:
        logger.info("Phase4.5: no empty-result candidates to repair")
        payload["phase4_5_summary"] = {"total_empty": 0, "repaired": 0, "new_candidates": 0}
        _save(payload, input_path)
        return

    logger.info(
        "Phase4.5: {} empty-result candidates across {} questions",
        len(jobs),
        len(set(s["question_id"] for s, _, _, _ in jobs)),
    )

    llm_timeout = int(getattr(args, "llm_timeout", 120))
    max_tokens = int(getattr(args, "max_tokens", Settings.MAX_TOKENS))

    total_new = 0
    total_repaired_questions = set()
    new_cands_by_qid = defaultdict(list)
    worker_timeout = int(getattr(args, "worker_timeout", 300))

    states: List[Dict[str, Any]] = []
    for job_idx, (sample, cand, idx, db_path) in enumerate(jobs):
        states.append(
            {
                "job_idx": job_idx,
                "sample": sample,
                "candidate": cand,
                "cand_idx": idx,
                "db_path": db_path,
                "current_sql": (cand.get("sql") or "").strip(),
                "history_lines": [],
                "new_candidates": [],
                "done": False,
                "diag": None,
                "diag_text": "",
                "has_cross_table_hit": False,
                "bank_entry": None,
            }
        )

    round_summaries = []
    for round_idx in range(1, max_rounds + 1):
        active_states = [state for state in states if not state["done"] and state.get("current_sql")]
        if not active_states:
            break

        logger.info(
            "Phase4.5 round {}: diagnose {} active candidates (bank={})",
            round_idx,
            len(active_states),
            bank.stats(),
        )
        for state in active_states:
            state["diag"] = None
            state["diag_text"] = ""
            state["has_cross_table_hit"] = False
            state["bank_entry"] = None

        diag_results, diag_timed_out = _run_jobs_with_deadline(
            active_states,
            lambda state: _diagnose_one_candidate(state, bank, exec_timeout),
            f"Phase4.5-R{round_idx}-Diagnose",
            threads,
            worker_timeout,
        )
        diagnosed_job_ids = set()
        for result in diag_results:
            state = states[result["job_idx"]]
            state["diag"] = result["diag"]
            state["diag_text"] = result["diag_text"]
            state["has_cross_table_hit"] = result["has_cross_table_hit"]
            state["bank_entry"] = result["bank_entry"]
            diagnosed_job_ids.add(result["job_idx"])

        repair_states = [state for state in active_states if state["job_idx"] in diagnosed_job_ids]
        logger.info(
            "Phase4.5 round {}: repair {} diagnosed candidates after prewarm",
            round_idx,
            len(repair_states),
        )
        repair_results, repair_timed_out = _run_jobs_with_deadline(
            repair_states,
            lambda state: _repair_one_candidate_round(
                state,
                bank,
                OpenAI(base_url=model_cfg["base_url"], api_key=model_cfg["api_key"]),
                model_cfg["model_name"],
                round_idx,
                exec_timeout,
                llm_timeout,
                max_tokens,
            ),
            f"Phase4.5-R{round_idx}-Repair",
            threads,
            worker_timeout,
        )

        round_new = 0
        round_repaired = 0
        for result in repair_results:
            state = states[result["job_idx"]]
            state["history_lines"].extend(result["history_updates"])
            state["current_sql"] = result["next_sql"]
            state["done"] = result["done"]
            state["new_candidates"].extend(result["new_candidates"])
            round_new += len(result["new_candidates"])
            if result["new_candidates"]:
                round_repaired += 1
            if result["done"]:
                total_repaired_questions.add(state["sample"]["question_id"])
            if result["fix_sql"] and state.get("bank_entry") is not None:
                state["bank_entry"].fix_sql = result["fix_sql"]
                state["bank_entry"].fix_succeeded = True

        total_new += round_new
        round_summaries.append(
            {
                "round": round_idx,
                "active_candidates": len(active_states),
                "diagnosed_candidates": len(diag_results),
                "diagnose_timeouts": len(diag_timed_out),
                "repaired_candidates": round_repaired,
                "new_candidates_generated": round_new,
                "repair_timeouts": len(repair_timed_out),
                "remaining_candidates": sum(
                    1 for state in states if not state["done"] and state.get("current_sql")
                ),
                "bank_entries": bank.size,
            }
        )
        logger.info(
            "Phase4.5 round {}: new_cands={} repaired={} remaining={} bank={}",
            round_idx,
            round_new,
            round_repaired,
            round_summaries[-1]["remaining_candidates"],
            bank.stats(),
        )

    for state in states:
        if state["new_candidates"]:
            new_cands_by_qid[state["sample"]["question_id"]].extend(state["new_candidates"])

    # Append new candidates to samples
    for sample in samples:
        qid = sample["question_id"]
        new_cands = new_cands_by_qid.get(qid, [])
        if not new_cands:
            continue
        # Dedup: don't add SQL that already exists
        existing = set(c.get("sql", "").strip() for c in sample.get("sql_candidates", []))
        for nc in new_cands:
            if nc["sql"].strip() not in existing:
                nc["index"] = len(sample["sql_candidates"])
                sample["sql_candidates"].append(nc)
                existing.add(nc["sql"].strip())

    # Summary
    summary = {
        "total_empty_candidates": len(jobs),
        "total_empty_questions": len(set(s["question_id"] for s, _, _, _ in jobs)),
        "new_candidates_generated": total_new,
        "questions_with_new_candidates": len(total_repaired_questions),
        "bank_final_size": bank.stats(),
        "model_key": model_key,
        "max_rounds": max_rounds,
        "round_summaries": round_summaries,
    }
    payload["phase4_5_summary"] = summary

    _save(payload, input_path)
    logger.info(
        "Phase4.5: done. empty={} new_cands={} repaired_questions={} bank={}",
        len(jobs),
        total_new,
        len(total_repaired_questions),
        bank.stats(),
    )


def _save(payload, input_path):
    tmp = input_path.with_suffix(".phase4_5.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(input_path)
