"""Collect errors from pipeline phase outputs into ErrorBank."""
from __future__ import annotations

import re
import time
from typing import Any, Dict, List

from error_bank.schema import ErrorEntry, ErrorType, DBFact


def _extract_tables_from_sql(sql: str) -> List[str]:
    """Extract table names from SQL using regex."""
    tables = set()
    for m in re.finditer(r'\bFROM\s+(\w+)', sql, re.I):
        tables.add(m.group(1))
    for m in re.finditer(r'\bJOIN\s+(\w+)', sql, re.I):
        tables.add(m.group(1))
    return sorted(tables)


def _extract_wrong_names_from_errors(errors: List[str]) -> tuple:
    """Extract wrong table/column names from Phase2 error messages."""
    wrong_tables = []
    wrong_columns = []
    for err in errors:
        m = re.search(r" '(\w+)' ", err)
        if m:
            wrong_tables.append(m.group(1))
        m = re.search(r'no such column:\s*(.+)', err, re.I)
        if m:
            wrong_columns.append(m.group(1).strip())
    return wrong_tables, wrong_columns


def collect_phase2_errors(sample: Dict[str, Any]) -> List[ErrorEntry]:
    """Extract errors from Phase 2 removed candidates."""
    entries = []
    qid = sample.get("question_id", 0)
    db_id = sample.get("db_id", "")
    question = sample.get("question", "")
    evidence = sample.get("evidence", "")

    for cand in sample.get("phase2_removed_candidates", []):
        reason = cand.get("removal_reason", "")
        errors = cand.get("validation", {}).get("errors", [])
        sql = cand.get("sql", "")

        # Map removal reason to ErrorType
        if reason == "no_such_column":
            etype = ErrorType.NO_SUCH_COLUMN
        elif reason == "no_such_table":
            etype = ErrorType.NO_SUCH_TABLE
        elif reason == "schema_validate_fail":
            etype = ErrorType.SCHEMA_VALIDATE_FAIL
        elif reason in ("bracket_mismatch", "quote_mismatch", "missing_from_or_select"):
            etype = ErrorType.SYNTAX_ERROR
        else:
            etype = ErrorType.UNKNOWN

        wrong_tables, wrong_columns = _extract_wrong_names_from_errors(errors)
        tables = _extract_tables_from_sql(sql)

        entries.append(ErrorEntry(
            question_id=qid,
            db_id=db_id,
            question=question,
            evidence=evidence,
            tables=tables,
            columns=wrong_columns,
            sql_text=sql,
            error_type=etype,
            error_phase="phase2",
            error_detail="; ".join(errors)[:300],
            wrong_names=wrong_tables + wrong_columns,
            timestamp=time.time(),
        ))

    return entries


def collect_phase3_errors(sample: Dict[str, Any]) -> List[ErrorEntry]:
    """Extract errors from Phase 3 execution diagnostics."""
    entries = []
    qid = sample.get("question_id", 0)
    db_id = sample.get("db_id", "")
    question = sample.get("question", "")
    evidence = sample.get("evidence", "")

    for cand in sample.get("sql_candidates", []):
        p3 = cand.get("phase3_diagnostics", {})
        failure_type = p3.get("failure_type", "clean")
        if failure_type == "clean":
            continue

        sql = cand.get("sql", "")

        if failure_type == "empty_result":
            etype = ErrorType.EMPTY_RESULT
        elif failure_type == "timeout":
            etype = ErrorType.TIMEOUT
        elif failure_type == "runtime_error":
            etype = ErrorType.RUNTIME_ERROR
        else:
            etype = ErrorType.UNKNOWN

        entries.append(ErrorEntry(
            question_id=qid,
            db_id=db_id,
            question=question,
            evidence=evidence,
            tables=_extract_tables_from_sql(sql),
            sql_text=sql,
            error_type=etype,
            error_phase="phase3",
            error_detail=p3.get("error_message", "")[:300],
            timestamp=time.time(),
        ))

    return entries


def collect_phase4_errors(sample: Dict[str, Any]) -> List[ErrorEntry]:
    """Extract errors from Phase 4 correction history."""
    entries = []
    qid = sample.get("question_id", 0)
    db_id = sample.get("db_id", "")
    question = sample.get("question", "")
    evidence = sample.get("evidence", "")

    for cand in sample.get("sql_candidates", []):
        for h in cand.get("phase4_correction_history", []):
            failure_type = h.get("failure_type", "")
            input_sql = h.get("input_sql", "")
            corrected_sql = h.get("corrected_sql", "")
            re_exec = h.get("re_execution", {})
            succeeded = (re_exec.get("status") == "succeeded"
                        and not re_exec.get("empty_result", False))

            if failure_type == "empty_result":
                etype = ErrorType.EMPTY_RESULT
            elif failure_type == "timeout":
                etype = ErrorType.TIMEOUT
            else:
                etype = ErrorType.RUNTIME_ERROR

            entries.append(ErrorEntry(
                question_id=qid,
                db_id=db_id,
                question=question,
                evidence=evidence,
                tables=_extract_tables_from_sql(input_sql),
                sql_text=input_sql,
                error_type=etype,
                error_phase="phase4",
                error_detail=f"{failure_type}: {h.get('error', '')}",
                fix_sql=corrected_sql,
                fix_succeeded=succeeded,
                timestamp=time.time(),
            ))

    return entries


def collect_probe_diagnosis(
    question_id: int,
    db_id: str,
    question: str,
    evidence: str,
    diagnosis,  # Diagnosis object from prober
) -> ErrorEntry:
    """Convert a probe Diagnosis into an ErrorEntry with DB facts."""
    # Map probe root cause to ErrorType
    rc = diagnosis.root_cause
    etype_map = {
        "FUZZY_MISMATCH": ErrorType.VALUE_MISMATCH,
        "CASE_MISMATCH": ErrorType.VALUE_MISMATCH,
        "SUFFIX_MISMATCH": ErrorType.VALUE_MISMATCH,
        "PREFIX_MATCH": ErrorType.VALUE_MISMATCH,
        "SUBSTRING_MATCH": ErrorType.VALUE_MISMATCH,
        "VALUE_NOT_EXISTS": ErrorType.VALUE_NOT_EXISTS,
        "EXACT_MATCH_EXISTS": ErrorType.VALUE_NOT_EXISTS,
        "JOIN_NO_OVERLAP": ErrorType.JOIN_ERROR,
        "JOIN_LOW_OVERLAP": ErrorType.JOIN_ERROR,
        "HAVING_TOO_RESTRICTIVE": ErrorType.HAVING_TOO_RESTRICTIVE,
        "FUNCTION_FILTER_KILLS": ErrorType.FUNCTION_FILTER_KILLS,
        "SUBQUERY_VALUE_MISMATCH": ErrorType.SUBQUERY_ERROR,
        "SUBQUERY_RETURNS_EMPTY": ErrorType.SUBQUERY_ERROR,
    }
    etype = etype_map.get(rc, ErrorType.EMPTY_RESULT)

    # Convert value probes to DBFact
    db_facts = []
    for vp in diagnosis.value_probes:
        db_facts.append(DBFact(
            column=vp.column,
            expected_value=vp.expected_value,
            actual_values=[str(v) for v in vp.actual_distinct_values[:10]],
            diagnosis=vp.diagnosis,
            closest_match=vp.closest_match,
            suggested_fix=vp.suggested_fix,
        ))

    return ErrorEntry(
        question_id=question_id,
        db_id=db_id,
        question=question,
        evidence=evidence,
        tables=list(set(a.split(".")[0] for a in diagnosis.schema_anchors if "." in a)),
        columns=diagnosis.schema_anchors,
        sql_text=diagnosis.original_sql,
        error_type=etype,
        error_phase="phase4.5_probe",
        error_detail=diagnosis.root_cause_detail[:300],
        killer_condition=diagnosis.killer_stage.condition_text if diagnosis.killer_stage else "",
        db_facts=db_facts,
        fix_sql=diagnosis.suggested_fix_sql,
        fix_succeeded=False,  # updated later when fix is applied
        timestamp=time.time(),
    )


def collect_from_pipeline(store, pipeline_data: Dict[str, Any]):
    """Bulk collect errors from a full pipeline run JSON."""
    count = 0
    for sample in pipeline_data.get("results", []):
        for e in collect_phase2_errors(sample):
            store.insert(e)
            count += 1
        for e in collect_phase3_errors(sample):
            store.insert(e)
            count += 1
        for e in collect_phase4_errors(sample):
            store.insert(e)
            count += 1
    return count
