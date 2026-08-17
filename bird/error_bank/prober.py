#!/usr/bin/env python3
"""Progressive Probing v2 — enhanced empty-result SQL diagnosis.

Improvements over v1:
  1. INTERSECT/UNION/EXCEPT: probe each branch independently
  2. CTE: expand and probe the main query with CTE context
  3. Subquery WHERE: execute subqueries independently
  4. IN conditions: per-value probing
  5. Function expressions (STRFTIME/SUBSTR): probe raw column values
  6. JOIN key errors: backtrack check when WHERE kills after JOIN
  7. NULL awareness: detect NULL-filtered columns
  8. Combination filter: probe each WHERE condition independently (not just cumulative)
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from config import Settings


# ── Data classes ──────────────────────────────────────────────────

@dataclass
class ProbeStageResult:
    stage_idx: int
    stage_type: str       # "from", "join", "where", "having", "branch"
    condition_text: str
    probe_sql: str
    row_count: int
    drop_from_prev: Optional[int] = None
    is_killer: bool = False


@dataclass
class ValueProbeResult:
    column: str
    expected_value: str
    actual_distinct_values: List[str]
    diagnosis: str
    closest_match: Optional[str] = None
    suggested_fix: Optional[str] = None


@dataclass
class JoinProbeResult:
    left_col: str
    right_col: str
    left_distinct_count: int
    right_distinct_count: int
    overlap_count: int
    diagnosis: str
    suggested_fix: Optional[str] = None


@dataclass
class Diagnosis:
    question_id: int
    db_id: str
    original_sql: str
    stages: List[ProbeStageResult] = field(default_factory=list)
    killer_stage: Optional[ProbeStageResult] = None
    value_probes: List[ValueProbeResult] = field(default_factory=list)
    join_probes: List[JoinProbeResult] = field(default_factory=list)
    root_cause: str = "UNKNOWN"
    root_cause_detail: str = ""
    schema_anchors: List[str] = field(default_factory=list)
    suggested_fix_sql: str = ""
    fix_description: str = ""


# ── DB helpers ────────────────────────────────────────────────────

def _exec_count(sql: str, db_path: str, timeout: int = 0) -> int:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
        start = time.perf_counter()
        if timeout > 0:
            def _progress_handler() -> None:
                if time.perf_counter() - start > timeout:
                    raise TimeoutError("probe_sql_timeout")
            conn.set_progress_handler(_progress_handler, 1000)
        cur = conn.cursor()
        cur.execute(sql)
        r = cur.fetchone()
        try:
            conn.set_progress_handler(None, 0)
        except Exception:
            pass
        conn.close()
        return int(r[0]) if r else 0
    except Exception:
        return -1


def _exec_values(sql: str, db_path: str, timeout: int = 0) -> List[Any]:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
        start = time.perf_counter()
        if timeout > 0:
            def _progress_handler() -> None:
                if time.perf_counter() - start > timeout:
                    raise TimeoutError("probe_sql_timeout")
            conn.set_progress_handler(_progress_handler, 1000)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        try:
            conn.set_progress_handler(None, 0)
        except Exception:
            pass
        conn.close()
        return [row[0] for row in rows]
    except Exception:
        return []


def _exec_rows(sql: str, db_path: str, timeout: int = 0) -> List[Tuple]:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
        start = time.perf_counter()
        if timeout > 0:
            def _progress_handler() -> None:
                if time.perf_counter() - start > timeout:
                    raise TimeoutError("probe_sql_timeout")
            conn.set_progress_handler(_progress_handler, 1000)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        try:
            conn.set_progress_handler(None, 0)
        except Exception:
            pass
        conn.close()
        return rows
    except Exception:
        return []


# ── SQL text helpers (regex-based, no sqlglot dependency issues) ──

def _split_set_operations(sql: str) -> Optional[Tuple[str, str, str]]:
    """Split SQL on top-level INTERSECT/UNION/EXCEPT. Returns (left, op, right) or None."""
    # Only split on top-level set ops (not inside subqueries)
    depth = 0
    upper = sql.upper()
    for op_keyword in ["INTERSECT", "EXCEPT", "UNION ALL", "UNION"]:
        i = 0
        while i < len(upper):
            if upper[i] == '(':
                depth += 1
            elif upper[i] == ')':
                depth -= 1
            elif depth == 0:
                pos = upper.find(op_keyword, i)
                if pos == i:
                    left = sql[:pos].strip()
                    right = sql[pos + len(op_keyword):].strip()
                    if left and right:
                        return left, op_keyword, right
            i += 1
        depth = 0
    return None


def _extract_ctes(sql: str) -> Tuple[str, str]:
    """Extract CTE prefix and main query. Returns (cte_prefix, main_query).
    If no CTE, returns ('', sql)."""
    stripped = sql.strip()
    if not stripped.upper().startswith("WITH"):
        return "", stripped

    # Find the main SELECT after CTE definitions
    # Track parentheses depth to find the end of CTE block
    depth = 0
    i = 0
    upper = stripped.upper()
    # Skip "WITH"
    i = 4
    while i < len(stripped):
        if stripped[i] == '(':
            depth += 1
        elif stripped[i] == ')':
            depth -= 1
        elif depth == 0 and i > 4:
            # Look for SELECT at top level after CTE
            rest = upper[i:].lstrip()
            if rest.startswith("SELECT"):
                cte_prefix = stripped[:i].rstrip().rstrip(',')
                main_query = stripped[i:].lstrip()
                return cte_prefix, main_query
        i += 1
    return "", stripped


def _resolve_alias(alias: str, sql: str) -> str:
    """Resolve table alias to actual table name."""
    patterns = [
        rf'(\w+)\s+AS\s+{re.escape(alias)}\b',
        rf'FROM\s+(\w+)\s+{re.escape(alias)}\b',
        rf'JOIN\s+(\w+)\s+{re.escape(alias)}\b',
    ]
    for pat in patterns:
        m = re.search(pat, sql, re.I)
        if m:
            return m.group(1)
    return alias


def _strip_trailing_semicolons(sql: str) -> str:
    """Remove trailing semicolons which break probe sub-queries."""
    return sql.rstrip().rstrip(';').rstrip()


def _extract_from_clause(sql: str) -> str:
    """Extract FROM ... up to WHERE/GROUP/HAVING/ORDER/LIMIT or end."""
    clean = _strip_trailing_semicolons(sql)
    m = re.search(
        r'\bFROM\b(.+?)(?:\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|$)',
        clean, re.I | re.S
    )
    return f"FROM{m.group(1)}" if m else ""


def _extract_where_block(sql: str) -> str:
    """Extract the WHERE clause content (between WHERE and GROUP BY/HAVING/ORDER BY/LIMIT)."""
    clean = _strip_trailing_semicolons(sql)
    m = re.search(
        r'\bWHERE\b\s+(.+?)(?:\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|$)',
        clean, re.I | re.S
    )
    return m.group(1).strip() if m else ""


def _split_and_conditions(where_text: str) -> List[str]:
    """Split WHERE clause on top-level AND (not inside parentheses or quoted strings)."""
    conditions = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    buf = ""
    i = 0
    text = where_text
    while i < len(text):
        ch = text[i]
        # Track quotes
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            buf += ch
        elif ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            buf += ch
        elif in_single_quote or in_double_quote:
            buf += ch
        elif ch == '(':
            depth += 1
            buf += ch
        elif ch == ')':
            depth -= 1
            buf += ch
        elif (depth == 0
              and text[i:i+5].upper() in (' AND ', '\nAND ')
              ):
            if buf.strip():
                conditions.append(buf.strip())
            buf = ""
            i += 5  # skip ' AND '
            continue
        elif (depth == 0 and i == 0
              and text[i:i+4].upper() == 'AND '
              ):
            i += 4
            continue
        else:
            buf += ch
        i += 1
    if buf.strip():
        conditions.append(buf.strip())
    return conditions


def _extract_group_by(sql: str) -> str:
    clean = _strip_trailing_semicolons(sql)
    m = re.search(r'(GROUP\s+BY\b[^)]*?)(?:\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|$)', clean, re.I | re.S)
    return m.group(1).strip() if m else ""


def _extract_having(sql: str) -> str:
    clean = _strip_trailing_semicolons(sql)
    m = re.search(r'\bHAVING\b\s+(.+?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)', clean, re.I | re.S)
    return m.group(1).strip() if m else ""


def _extract_join_pairs(from_clause: str) -> List[Tuple[str, str, str]]:
    """Extract (join_text, left_col, right_col) from FROM clause."""
    pairs = []
    for m in re.finditer(r'((?:INNER\s+|LEFT\s+|RIGHT\s+|CROSS\s+)?JOIN\s+\w+(?:\s+(?:AS\s+)?\w+)?)\s+ON\s+(\S+)\s*=\s*(\S+)', from_clause, re.I):
        pairs.append((m.group(0), m.group(2), m.group(3)))
    return pairs


def _extract_subquery(cond: str) -> Optional[str]:
    """If condition contains a subquery (SELECT ...), extract it."""
    m = re.search(r'\(\s*(SELECT\s.+)\)', cond, re.I | re.S)
    if m:
        return m.group(1).strip()
    return None


def _parse_in_values(cond: str) -> Optional[Tuple[str, List[str]]]:
    """Parse 'col IN ('v1', 'v2', ...)' → (col, [v1, v2])."""
    m = re.match(r"([\w.`\"\[\]]+)\s+IN\s*\(([^)]+)\)", cond, re.I)
    if m:
        col = m.group(1).strip().strip('`"[]')
        vals_str = m.group(2)
        # Check if it's a subquery
        if vals_str.strip().upper().startswith("SELECT"):
            return None
        vals = re.findall(r"'([^']*)'", vals_str)
        if not vals:
            vals = [v.strip().strip("'\"") for v in vals_str.split(",")]
        return col, vals
    return None


def _parse_equality(cond: str) -> Optional[Tuple[str, str]]:
    """Parse 'col = value' or 'col = number'."""
    # Skip complex expressions (functions, subqueries)
    if '(' in cond.split('=')[0] if '=' in cond else True:
        # left side has function call
        pass

    m = re.match(r"""([\w."`\[\]]+)\s*=\s*'([^']*)'""", cond.strip())
    if m:
        return m.group(1).strip('`"[]'), m.group(2)
    m = re.match(r"""([\w."`\[\]]+)\s*=\s*(\d+(?:\.\d+)?)""", cond.strip())
    if m:
        return m.group(1).strip('`"[]'), m.group(2)
    return None


def _has_function_call(cond: str) -> bool:
    """Check if the condition's left side contains a function call."""
    eq_pos = cond.find('=')
    if eq_pos < 0:
        return False
    left = cond[:eq_pos]
    return '(' in left


def _extract_function_and_column(cond: str) -> Optional[Tuple[str, str, str, str]]:
    """Extract function(column) = value patterns.
    Returns (function_name, table_alias, column, expected_value) or None."""
    # STRFTIME('%Y-%m', T2.Date) = '2013-09'
    m = re.match(r"""(\w+)\s*\([^,]*?(?:(\w+)\.)?(\w+)\s*(?:,[^)]+)?\)\s*=\s*'([^']*)'""", cond.strip(), re.I)
    if m:
        return m.group(1), m.group(2) or "", m.group(3), m.group(4)
    # SUBSTR(T1.Date, 1, 4) = '2013'
    m = re.match(r"""(\w+)\s*\(\s*(?:(\w+)\.)?(\w+)\s*,.+?\)\s*=\s*'([^']*)'""", cond.strip(), re.I)
    if m:
        return m.group(1), m.group(2) or "", m.group(3), m.group(4)
    return None


def _fuzzy_match(expected: str, candidates: List[str]) -> Optional[Tuple[str, float, str]]:
    """Find best fuzzy match. Returns (match, score, diagnosis_type) or None."""
    if not candidates:
        return None

    best_match = None
    best_score = 0.0
    best_type = "VALUE_NOT_EXISTS"

    for cand in candidates:
        cand_str = str(cand)

        # Exact match
        if cand_str == expected:
            return cand_str, 1.0, "EXACT_MATCH_EXISTS"

        # Case mismatch
        if cand_str.lower() == expected.lower():
            return cand_str, 0.95, "CASE_MISMATCH"

        # Prefix/suffix match (e.g., "1:15" matches "1:15.110")
        if cand_str.startswith(expected) or expected.startswith(cand_str):
            score = min(len(expected), len(cand_str)) / max(len(expected), len(cand_str))
            if score > best_score:
                best_match, best_score, best_type = cand_str, score, "PREFIX_MATCH"

        # Contains match (e.g., date with timestamp)
        if expected in cand_str or cand_str in expected:
            score = min(len(expected), len(cand_str)) / max(len(expected), len(cand_str))
            if score > best_score:
                best_match, best_score, best_type = cand_str, score, "SUBSTRING_MATCH"

        # Whitespace/trailing difference
        if cand_str.strip().rstrip('s') == expected.strip().rstrip('s'):
            return cand_str, 0.9, "SUFFIX_MISMATCH"

        # Edit distance
        ratio = SequenceMatcher(None, cand_str.lower(), expected.lower()).ratio()
        if ratio > best_score and ratio > 0.6:
            best_match, best_score, best_type = cand_str, ratio, "FUZZY_MISMATCH"

    if best_match and best_score > 0.5:
        return best_match, best_score, best_type
    return None


# ── Progressive Probing v2 ────────────────────────────────────────

def _probe_single_branch(sql: str, db_path: str, branch_label: str, cte_sql_prefix: str = "", timeout: int = 0) -> List[ProbeStageResult]:
    """Probe a single SELECT statement (no set operations).
    If cte_sql_prefix is provided, prepend it to all probe queries."""
    stages: List[ProbeStageResult] = []
    clean_sql = _strip_trailing_semicolons(sql)
    from_clause = _extract_from_clause(clean_sql)
    if not from_clause:
        return stages

    where_text = _extract_where_block(clean_sql)
    group_by = _extract_group_by(clean_sql)
    having_text = _extract_having(clean_sql)

    def _probe(probe_body: str) -> int:
        """Execute a count probe, prepending CTE if needed."""
        full = f"{cte_sql_prefix}\n{probe_body}" if cte_sql_prefix else probe_body
        return _exec_count(full, db_path, timeout=timeout)

    # Stage 0: full FROM (including JOINs) without WHERE
    count_sql = f"SELECT COUNT(*) {from_clause}"
    count = _probe(count_sql)
    stages.append(ProbeStageResult(
        stage_idx=0, stage_type="from",
        condition_text=from_clause[:120],
        probe_sql=count_sql, row_count=count,
    ))

    # If FROM itself has JOINs, probe them incrementally
    join_pairs = _extract_join_pairs(from_clause)
    if len(join_pairs) > 1:
        base_m = re.match(r'FROM\s+(\S+(?:\s+(?:AS\s+)?\w+)?)', from_clause, re.I)
        if base_m:
            incremental = f"FROM {base_m.group(1)}"
            base_count = _probe(f"SELECT COUNT(*) {incremental}")
            stages[0] = ProbeStageResult(
                stage_idx=0, stage_type="from",
                condition_text=incremental[:120],
                probe_sql=f"SELECT COUNT(*) {incremental}",
                row_count=base_count,
            )
            for jp_text, lc, rc in join_pairs:
                incremental += f" {jp_text}"
                c = _probe(f"SELECT COUNT(*) {incremental}")
                prev = stages[-1].row_count
                drop = prev - c if prev >= 0 and c >= 0 else None
                stages.append(ProbeStageResult(
                    stage_idx=len(stages), stage_type="join",
                    condition_text=f"{jp_text[:100]} (ON {lc}={rc})",
                    probe_sql=f"SELECT COUNT(*) {incremental}",
                    row_count=c, drop_from_prev=drop,
                ))

    # WHERE conditions — probe each INDEPENDENTLY first, then cumulatively
    if where_text:
        conditions = _split_and_conditions(where_text)

        independent_kills = []
        for cond in conditions:
            c = _probe(f"SELECT COUNT(*) {from_clause} WHERE {cond}")
            if c == 0:
                independent_kills.append(cond)

        cumul = ""
        for i, cond in enumerate(conditions):
            cumul = f"{cumul} AND {cond}" if cumul else cond
            probe_sql = f"SELECT COUNT(*) {from_clause} WHERE {cumul}"
            c = _probe(probe_sql)
            prev = stages[-1].row_count
            drop = prev - c if prev >= 0 and c >= 0 else None
            is_indep_killer = cond in independent_kills
            label = f"{'[!SOLO_KILL] ' if is_indep_killer else ''}{cond[:100]}"
            stages.append(ProbeStageResult(
                stage_idx=len(stages), stage_type="where",
                condition_text=label,
                probe_sql=probe_sql, row_count=c, drop_from_prev=drop,
            ))

    # HAVING
    if having_text and group_by:
        full_where = f"WHERE {where_text}" if where_text else ""
        probe_sql = f"SELECT COUNT(*) FROM (SELECT 1 {from_clause} {full_where} {group_by} HAVING {having_text})"
        c = _probe(probe_sql)
        prev = stages[-1].row_count
        drop = prev - c if prev >= 0 and c >= 0 else None
        stages.append(ProbeStageResult(
            stage_idx=len(stages), stage_type="having",
            condition_text=f"HAVING {having_text[:100]}",
            probe_sql=probe_sql, row_count=c, drop_from_prev=drop,
        ))

    # Mark killer
    prev_count = None
    for s in stages:
        if prev_count is not None and prev_count > 0 and s.row_count == 0:
            s.is_killer = True
            break
        if s.row_count >= 0:
            prev_count = s.row_count

    return stages


def progressive_probe(sql: str, db_path: str, timeout: int = 0) -> Tuple[List[ProbeStageResult], str]:
    """Top-level probe dispatcher. Returns (stages, probe_mode)."""

    # 1. Handle CTE: prepend CTE as context
    cte_prefix, main_sql = _extract_ctes(sql)

    # 2. Handle INTERSECT/UNION/EXCEPT
    set_split = _split_set_operations(main_sql)
    if set_split:
        left_sql, op, right_sql = set_split
        # Add CTE prefix back for each branch
        if cte_prefix:
            left_full = f"{cte_prefix}\n{left_sql}"
            right_full = f"{cte_prefix}\n{right_sql}"
        else:
            left_full = left_sql
            right_full = right_sql

        stages = []
        # Probe left branch
        left_count = _exec_count(f"SELECT COUNT(*) FROM ({left_full})", db_path, timeout=timeout)
        stages.append(ProbeStageResult(
            stage_idx=0, stage_type="branch",
            condition_text=f"LEFT branch of {op}",
            probe_sql=f"SELECT COUNT(*) FROM ({left_full})",
            row_count=left_count,
        ))
        left_stages = _probe_single_branch(left_full, db_path, "LEFT", timeout=timeout)
        for s in left_stages:
            s.condition_text = f"[L] {s.condition_text}"
            s.stage_idx = len(stages)
            stages.append(s)

        # Probe right branch
        right_count = _exec_count(f"SELECT COUNT(*) FROM ({right_full})", db_path, timeout=timeout)
        stages.append(ProbeStageResult(
            stage_idx=len(stages), stage_type="branch",
            condition_text=f"RIGHT branch of {op}",
            probe_sql=f"SELECT COUNT(*) FROM ({right_full})",
            row_count=right_count,
        ))
        right_stages = _probe_single_branch(right_full, db_path, "RIGHT", timeout=timeout)
        for s in right_stages:
            s.condition_text = f"[R] {s.condition_text}"
            s.stage_idx = len(stages)
            stages.append(s)

        # The overall result
        full_count = _exec_count(f"SELECT COUNT(*) FROM ({sql})", db_path, timeout=timeout)
        stages.append(ProbeStageResult(
            stage_idx=len(stages), stage_type="branch",
            condition_text=f"{op} result",
            probe_sql=f"SELECT COUNT(*) FROM ({sql})",
            row_count=full_count,
        ))

        # Determine which branch is the problem
        if left_count == 0 and right_count == 0:
            # Both branches empty — mark killers from each
            pass  # individual branch stages already have killers
        elif left_count == 0:
            stages[0].is_killer = True
        elif right_count == 0:
            # For INTERSECT, right being empty kills; for UNION, it doesn't
            if op == "INTERSECT":
                for s in stages:
                    if s.condition_text.startswith("RIGHT branch"):
                        s.is_killer = True
                        break

        return stages, f"SET_OP:{op}"

    # 3. Handle CTE in single query: probe the main query with CTE context
    if cte_prefix:
        # Probe the CTE subquery first
        cte_stages = []
        # Extract CTE name and query
        cte_m = re.search(r'WITH\s+(\w+)\s+AS\s*\((.*)\)', cte_prefix, re.I | re.S)
        if cte_m:
            cte_name = cte_m.group(1)
            cte_query = cte_m.group(2).strip()
            cte_count = _exec_count(f"SELECT COUNT(*) FROM ({cte_query})", db_path, timeout=timeout)
            cte_stages.append(ProbeStageResult(
                stage_idx=0, stage_type="branch",
                condition_text=f"CTE '{cte_name}' result",
                probe_sql=f"SELECT COUNT(*) FROM ({cte_query})",
                row_count=cte_count,
            ))
            if cte_count == 0:
                cte_stages[0].is_killer = True

        # Probe main query WITH CTE prefix prepended to all probes
        main_stages = _probe_single_branch(sql, db_path, "main_with_cte", cte_sql_prefix=cte_prefix, timeout=timeout)
        for s in main_stages:
            s.stage_idx += len(cte_stages)
        all_stages = cte_stages + main_stages
        return all_stages, "CTE"

    # 4. Regular single query
    return _probe_single_branch(sql, db_path, "main", timeout=timeout), "SINGLE"


# ── Deep Probing on Killer ────────────────────────────────────────

def _deep_probe_where_condition(
    cond: str, from_clause: str, sql: str, db_path: str, timeout: int = 0
) -> Tuple[str, str, List[ValueProbeResult], List[str], str]:
    """Deep probe a WHERE condition. Returns (root_cause, detail, value_probes, anchors, fix)."""
    value_probes = []
    anchors = []
    fix = ""

    # --- Case 1: IN condition with literal values ---
    in_parsed = _parse_in_values(cond)
    if in_parsed:
        col, vals = in_parsed
        parts = col.split(".")
        actual_table = _resolve_alias(parts[0], sql) if len(parts) == 2 else ""
        col_name = parts[-1]

        if actual_table:
            actual_vals = [str(v) for v in _exec_values(
                f"SELECT DISTINCT [{col_name}] FROM [{actual_table}] WHERE [{col_name}] IS NOT NULL LIMIT 50",
                db_path,
                timeout=timeout,
            )]
        else:
            actual_vals = []

        # Check each IN value
        missing_vals = []
        for v in vals:
            if v not in actual_vals:
                match = _fuzzy_match(v, actual_vals)
                if match:
                    missing_vals.append((v, match[0], match[2]))
                else:
                    missing_vals.append((v, None, "NOT_FOUND"))

        if missing_vals:
            detail_parts = [f"'{v}' → closest='{m}' ({t})" if m else f"'{v}' → NOT_FOUND"
                           for v, m, t in missing_vals]
            vp = ValueProbeResult(
                column=col, expected_value=str(vals),
                actual_distinct_values=actual_vals[:10],
                diagnosis="IN_VALUES_MISMATCH",
                closest_match=str([(v, m) for v, m, _ in missing_vals if m]),
            )
            value_probes.append(vp)
            if actual_table:
                anchors.append(f"{actual_table}.{col_name}")
            return "IN_VALUES_MISMATCH", f"IN condition: {', '.join(detail_parts)}. Actual: {actual_vals[:8]}", value_probes, anchors, fix

    # --- Case 2: Function expression (STRFTIME, SUBSTR, LOWER, etc.) ---
    func_parsed = _extract_function_and_column(cond)
    if func_parsed:
        func_name, table_alias, col_name, expected = func_parsed
        actual_table = _resolve_alias(table_alias, sql) if table_alias else ""

        if actual_table:
            # Get raw values
            raw_vals = [str(v) for v in _exec_values(
                f"SELECT DISTINCT [{col_name}] FROM [{actual_table}] WHERE [{col_name}] IS NOT NULL LIMIT 20",
                db_path,
                timeout=timeout,
            )]
            # Also try applying the function
            func_vals = [str(v) for v in _exec_values(
                f"SELECT DISTINCT {func_name}({cond.split(func_name)[1].split(')')[0]}), [{col_name}]) FROM [{actual_table}] WHERE [{col_name}] IS NOT NULL LIMIT 20",
                db_path,
                timeout=timeout,
            )]
            # Simpler: just run the original function
            test_sql = f"SELECT DISTINCT {cond.split('=')[0].strip()} {from_clause} LIMIT 20"
            func_results = [str(v) for v in _exec_values(test_sql, db_path, timeout=timeout)]

            vp = ValueProbeResult(
                column=f"{actual_table}.{col_name}",
                expected_value=expected,
                actual_distinct_values=func_results[:10] if func_results else raw_vals[:10],
                diagnosis="FUNCTION_VALUE_MISMATCH",
            )

            match = _fuzzy_match(expected, func_results)
            if match:
                vp.closest_match = match[0]
                vp.diagnosis = f"FUNCTION_{match[2]}"
                vp.suggested_fix = f"Use raw column value or adjust function. Closest: '{match[0]}'"
            elif not func_results and raw_vals:
                vp.diagnosis = "FUNCTION_RETURNS_NULL"
                vp.suggested_fix = f"Function {func_name}() returns NULL. Raw values: {raw_vals[:5]}"

            value_probes.append(vp)
            anchors.append(f"{actual_table}.{col_name}")
            return vp.diagnosis, f"{func_name}({col_name}) expected '{expected}'. Function outputs: {func_results[:5]}. Raw values: {raw_vals[:5]}", value_probes, anchors, vp.suggested_fix or ""

    # --- Case 3: Subquery in condition ---
    subquery = _extract_subquery(cond)
    if subquery:
        sub_count = _exec_count(f"SELECT COUNT(*) FROM ({subquery})", db_path, timeout=timeout)
        sub_vals = [str(v) for v in _exec_values(f"SELECT * FROM ({subquery}) LIMIT 10", db_path, timeout=timeout)]
        if sub_count == 0:
            return "SUBQUERY_RETURNS_EMPTY", f"Subquery returns 0 rows: {subquery[:150]}", value_probes, anchors, ""
        else:
            return "SUBQUERY_VALUE_MISMATCH", f"Subquery returns {sub_count} rows: {sub_vals[:5]}, but no match in outer query", value_probes, anchors, ""

    # --- Case 4: Simple equality ---
    eq = _parse_equality(cond)
    if eq:
        col, val = eq
        parts = col.split(".")
        actual_table = _resolve_alias(parts[0], sql) if len(parts) == 2 else ""
        col_name = parts[-1]

        if actual_table:
            actual_vals = [str(v) for v in _exec_values(
                f"SELECT DISTINCT [{col_name}] FROM [{actual_table}] WHERE [{col_name}] IS NOT NULL LIMIT 30",
                db_path,
                timeout=timeout,
            )]
        else:
            actual_vals = []

        match = _fuzzy_match(val, actual_vals)
        if match:
            closest, score, match_type = match
            vp = ValueProbeResult(col, val, actual_vals[:10], match_type, closest)
            if match_type == "CASE_MISMATCH":
                vp.suggested_fix = f"Use '{closest}' or LOWER([{col_name}]) = '{val.lower()}'"
            elif match_type == "PREFIX_MATCH":
                vp.suggested_fix = f"Use LIKE '{val}%' or exact value '{closest}'"
            elif match_type == "SUBSTRING_MATCH":
                vp.suggested_fix = f"Use LIKE '%{val}%' or SUBSTR comparison. Actual: '{closest}'"
            elif match_type == "SUFFIX_MISMATCH":
                vp.suggested_fix = f"Check trailing characters. Expected '{val}', actual '{closest}'"
            elif match_type == "FUZZY_MISMATCH":
                vp.suggested_fix = f"Possible typo. Expected '{val}', closest: '{closest}' (similarity={score:.2f})"
            elif match_type == "EXACT_MATCH_EXISTS":
                vp.suggested_fix = None
            value_probes.append(vp)
            anchors.append(f"{actual_table}.{col_name}" if actual_table else col_name)
            detail = f"[{col}] expected '{val}', actual values: {actual_vals[:5]}"
            if closest:
                detail += f". Closest: '{closest}'"
            return match_type, detail, value_probes, anchors, vp.suggested_fix or ""
        else:
            # Value truly doesn't exist
            # Check NULL ratio
            if actual_table:
                null_count = _exec_count(
                    f"SELECT COUNT(*) FROM [{actual_table}] WHERE [{col_name}] IS NULL", db_path, timeout=timeout
                )
                total = _exec_count(f"SELECT COUNT(*) FROM [{actual_table}]", db_path, timeout=timeout)
                null_pct = null_count / total * 100 if total > 0 else 0
            else:
                null_pct = 0

            vp = ValueProbeResult(col, val, actual_vals[:10], "VALUE_NOT_EXISTS")
            value_probes.append(vp)
            detail = f"[{col}] = '{val}' not found. Actual: {actual_vals[:8]}"
            if null_pct > 50:
                detail += f". WARNING: {null_pct:.0f}% of values are NULL"
            return "VALUE_NOT_EXISTS", detail, value_probes, anchors, ""

    # --- Case 5: Has function but we couldn't parse it cleanly ---
    if _has_function_call(cond):
        # Try to execute the condition expression directly
        test_sql = f"SELECT COUNT(*) {from_clause} WHERE {cond}"
        c = _exec_count(test_sql, db_path, timeout=timeout)
        # Try without the function (relax)
        return "FUNCTION_FILTER_KILLS", f"Complex function condition kills all rows: {cond[:120]}", value_probes, anchors, ""

    return "COMPLEX_WHERE_KILLER", f"Cannot parse condition: {cond[:120]}", value_probes, anchors, ""


def _deep_probe_join(join_text: str, sql: str, db_path: str, timeout: int = 0
                     ) -> Tuple[str, str, List[JoinProbeResult], List[str]]:
    """Deep probe a JOIN that killed results."""
    probes = []
    anchors = []

    m = re.search(r'ON\s+(\S+)\s*=\s*(\S+)', join_text, re.I)
    if not m:
        return "JOIN_PARSE_ERROR", f"Cannot parse JOIN ON: {join_text[:100]}", probes, anchors

    left, right = m.group(1), m.group(2)

    # Resolve aliases
    lp = left.split(".")
    rp = right.split(".")
    l_table = _resolve_alias(lp[0], sql) if len(lp) == 2 else lp[0]
    l_col = lp[-1]
    r_table = _resolve_alias(rp[0], sql) if len(rp) == 2 else rp[0]
    r_col = rp[-1]

    l_count = _exec_count(f"SELECT COUNT(DISTINCT [{l_col}]) FROM [{l_table}]", db_path, timeout=timeout)
    r_count = _exec_count(f"SELECT COUNT(DISTINCT [{r_col}]) FROM [{r_table}]", db_path, timeout=timeout)

    overlap = _exec_count(f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT [{l_col}] FROM [{l_table}]
            INTERSECT
            SELECT DISTINCT [{r_col}] FROM [{r_table}]
        )
    """, db_path, timeout=timeout)

    if overlap == 0:
        diag = "JOIN_NO_OVERLAP"
        detail = f"0 overlap between {l_table}.{l_col} ({l_count} distinct) and {r_table}.{r_col} ({r_count} distinct). Wrong JOIN key?"
    elif overlap < min(l_count, r_count) * 0.05:
        diag = "JOIN_LOW_OVERLAP"
        detail = f"Only {overlap} overlapping values. {l_table}.{l_col}={l_count}, {r_table}.{r_col}={r_count}"
    else:
        diag = "JOIN_OK_CHECK_WHERE"
        detail = f"JOIN overlap OK ({overlap}). Issue likely in subsequent WHERE conditions."

    jp = JoinProbeResult(f"{l_table}.{l_col}", f"{r_table}.{r_col}", l_count, r_count, overlap, diag)
    probes.append(jp)
    anchors.extend([f"{l_table}.{l_col}", f"{r_table}.{r_col}"])

    return diag, detail, probes, anchors


# ── Full Diagnosis Pipeline v2 ────────────────────────────────────

def diagnose_empty_result(question_id: int, db_id: str, sql: str, db_path: str, timeout: int = 0) -> Diagnosis:
    diag = Diagnosis(question_id=question_id, db_id=db_id, original_sql=sql)

    # Step 1: Progressive probing
    stages, probe_mode = progressive_probe(sql, db_path, timeout=timeout)
    diag.stages = stages

    # Find killer stage
    killer = None
    for s in stages:
        if s.is_killer:
            killer = s
            break
    diag.killer_stage = killer

    # If no killer found, check for special cases
    if not killer:
        # Check if any stage has -1 (probe error) followed by 0
        has_error_then_zero = False
        prev_was_error = False
        for s in stages:
            if s.row_count == -1:
                prev_was_error = True
            elif prev_was_error and s.row_count == 0:
                has_error_then_zero = True
                break
            else:
                prev_was_error = False

        # Check last stage
        if stages and stages[-1].row_count > 0:
            diag.root_cause = "NOT_ACTUALLY_EMPTY"
            diag.root_cause_detail = f"Probe shows {stages[-1].row_count} rows at last stage. Issue may be in SELECT DISTINCT, LIMIT, or outer set operation."
        elif stages and stages[-1].row_count == -1:
            # Last probe failed — try executing the full SQL as count
            full_count = _exec_count(f"SELECT COUNT(*) FROM ({sql})", db_path, timeout=timeout)
            if full_count > 0:
                diag.root_cause = "PROBE_DECOMPOSITION_ERROR"
                diag.root_cause_detail = f"Full query returns {full_count} rows but decomposed probe failed."
            elif full_count == 0:
                diag.root_cause = "CONFIRMED_EMPTY_PROBE_PARTIAL"
                diag.root_cause_detail = "Query confirmed empty. Some probe stages failed but result is 0."
                # Find the last successful stage with > 0 rows and the first failed stage
                last_good = None
                first_fail = None
                for s in stages:
                    if s.row_count > 0:
                        last_good = s
                    elif s.row_count == -1 and last_good:
                        first_fail = s
                        break
                if first_fail:
                    diag.root_cause_detail += f" Issue near: {first_fail.condition_text[:100]}"
            else:
                diag.root_cause = "PROBE_FAILED"
        elif stages and stages[0].row_count == 0:
            diag.root_cause = "EMPTY_BASE_TABLE"
            diag.root_cause_detail = f"Base table returns 0 rows"
        else:
            diag.root_cause = "PROBE_FAILED"
        return diag

    # Step 2: Deep probe on killer
    if killer.stage_type == "where":
        # Extract the raw condition text (strip labels)
        cond = killer.condition_text
        for prefix in ["[!SOLO_KILL] ", "[L] ", "[R] "]:
            cond = cond.replace(prefix, "")

        from_clause = _extract_from_clause(sql)
        # If CTE, use full SQL for FROM context
        cte_prefix, _ = _extract_ctes(sql)
        if cte_prefix:
            from_clause = _extract_from_clause(sql)

        root_cause, detail, vps, anchors, fix = _deep_probe_where_condition(
            cond, from_clause, sql, db_path, timeout=timeout
        )
        diag.root_cause = root_cause
        diag.root_cause_detail = detail
        diag.value_probes = vps
        diag.schema_anchors = anchors
        diag.fix_description = fix

    elif killer.stage_type == "join":
        cond = killer.condition_text
        for prefix in ["[L] ", "[R] "]:
            cond = cond.replace(prefix, "")
        root_cause, detail, jps, anchors = _deep_probe_join(cond, sql, db_path, timeout=timeout)
        diag.root_cause = root_cause
        diag.root_cause_detail = detail
        diag.join_probes = jps
        diag.schema_anchors = anchors

    elif killer.stage_type == "having":
        diag.root_cause = "HAVING_TOO_RESTRICTIVE"
        having_text = killer.condition_text.replace("HAVING ", "", 1)
        from_clause = _extract_from_clause(sql)
        where_text = _extract_where_block(sql)
        group_by = _extract_group_by(sql)

        # Probe: what does the HAVING expression actually produce?
        full_where = f"WHERE {where_text}" if where_text else ""
        probe = f"SELECT {having_text.split('=')[0].split('>')[0].split('<')[0].strip()} {from_clause} {full_where} {group_by} LIMIT 10"
        having_vals = [str(v) for v in _exec_values(probe, db_path, timeout=timeout)]
        diag.root_cause_detail = f"HAVING {having_text} filters all groups. Actual values of aggregation: {having_vals[:10]}"

    elif killer.stage_type == "branch":
        cond = killer.condition_text
        if "LEFT" in cond:
            diag.root_cause = "SET_OP_LEFT_EMPTY"
            diag.root_cause_detail = "Left branch of set operation returns 0 rows"
        elif "RIGHT" in cond:
            diag.root_cause = "SET_OP_RIGHT_EMPTY"
            diag.root_cause_detail = "Right branch of set operation returns 0 rows"
        # Look for sub-killers in branch stages and deep probe them
        for s in stages:
            if s.is_killer and s.stage_type != "branch":
                diag.root_cause_detail += f". Sub-killer: {s.condition_text[:100]}"
                # Deep probe the sub-killer WHERE condition
                if s.stage_type == "where":
                    sub_cond = s.condition_text
                    for prefix in ["[!SOLO_KILL] ", "[L] ", "[R] "]:
                        sub_cond = sub_cond.replace(prefix, "")
                    from_clause = _extract_from_clause(sql)
                    rc, detail, vps, anchors, fix = _deep_probe_where_condition(
                        sub_cond, from_clause, sql, db_path, timeout=timeout
                    )
                    diag.root_cause = f"SET_OP_BRANCH_{rc}"
                    diag.root_cause_detail += f"\n  Branch sub-diagnosis: {detail}"
                    diag.value_probes.extend(vps)
                    diag.schema_anchors.extend(anchors)
                    if fix:
                        diag.fix_description = fix
                break

    return diag


# ── Pretty Print ──────────────────────────────────────────────────

def print_diagnosis(diag: Diagnosis, question: str = "", evidence: str = "", gt_sql: str = ""):
    print("=" * 90)
    print(f"QID: {diag.question_id}  DB: {diag.db_id}")
    if question:
        print(f"Q: {question[:150]}")
    if evidence:
        print(f"Evidence: {evidence[:150]}")
    print(f"\nOriginal SQL:\n  {diag.original_sql[:300]}")
    if gt_sql:
        print(f"\nGT SQL:\n  {gt_sql[:300]}")

    print(f"\n--- Filter Chain Probe ---")
    for s in diag.stages:
        marker = " ◀◀◀ KILLER" if s.is_killer else ""
        drop_str = f" (dropped {s.drop_from_prev})" if s.drop_from_prev and s.drop_from_prev > 0 else ""
        count_str = f"{s.row_count:>8}" if s.row_count >= 0 else "   ERROR"
        print(f"  Stage {s.stage_idx:>2} [{s.stage_type:>7}]: {count_str} rows{drop_str}  | {s.condition_text[:80]}{marker}")

    print(f"\n--- Diagnosis ---")
    print(f"  Root Cause: {diag.root_cause}")
    print(f"  Detail: {diag.root_cause_detail[:300]}")
    if diag.fix_description:
        print(f"  Suggested Fix: {diag.fix_description}")

    if diag.value_probes:
        for vp in diag.value_probes:
            print(f"\n  Value Probe [{vp.column}]:")
            print(f"    Expected: '{vp.expected_value}'")
            print(f"    Actual: {vp.actual_distinct_values[:8]}")
            print(f"    Diagnosis: {vp.diagnosis}")
            if vp.closest_match:
                print(f"    Closest: {vp.closest_match}")
            if vp.suggested_fix:
                print(f"    Fix: {vp.suggested_fix}")

    if diag.join_probes:
        for jp in diag.join_probes:
            print(f"\n  Join Probe [{jp.left_col} ⟷ {jp.right_col}]:")
            print(f"    Left: {jp.left_distinct_count}, Right: {jp.right_distinct_count}, Overlap: {jp.overlap_count}")
            print(f"    Diagnosis: {jp.diagnosis}")

    if diag.schema_anchors:
        print(f"\n  Schema Anchors: {diag.schema_anchors}")
    print("=" * 90)
    print()


# ── Main ──────────────────────────────────────────────────────────

def main():
    exp_path = Path("experiments/experiments/dev_run_iquest_gemini_phase3_gemini_phase5_refined_rules_depth1")
    gt_path = Path("data/dev_20240627/dev.json")

    data = json.loads(exp_path.read_text(encoding="utf-8"))
    gt_rows = json.loads(gt_path.read_text(encoding="utf-8"))
    gt_map = {int(r["question_id"]): r for r in gt_rows}

    results = data["results"]
    diagnosed_count = 0
    success_count = 0
    fail_count = 0

    for sample in results:
        qid = sample["question_id"]
        db_id = sample["db_id"]
        question = sample["question"]
        evidence = sample.get("evidence", "")
        gt_info = gt_map.get(qid, {})
        gt_sql = gt_info.get("SQL", "")

        try:
            db_path = str(Settings.get_database_path(db_id))
        except FileNotFoundError:
            continue

        for cand in sample.get("sql_candidates", []):
            if not cand.get("execution", {}).get("empty_result"):
                continue

            sql = cand.get("sql", "").strip()
            if not sql:
                continue

            diag = diagnose_empty_result(qid, db_id, sql, db_path)
            print_diagnosis(diag, question=question, evidence=evidence, gt_sql=gt_sql)
            diagnosed_count += 1

            if diag.root_cause not in ("PROBE_FAILED", "UNKNOWN"):
                success_count += 1
            else:
                fail_count += 1

            break  # one per question

    print(f"\n{'='*60}")
    print(f"SUMMARY: {diagnosed_count} diagnosed, {success_count} successful ({success_count/diagnosed_count*100:.0f}%), {fail_count} failed")


if __name__ == "__main__":
    main()
