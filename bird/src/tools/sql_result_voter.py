"""Run SQL execution voting across all generated candidates."""

from __future__ import annotations

import random
import sqlite3
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple

from func_timeout import FunctionTimedOut, func_timeout
from loguru import logger


ExecutionRecord = Dict[str, Any]


def weighted_majority_vote(
    records: List[Dict[str, Any]],
    voting_weights: Sequence[float] | None = None,
    triple_vote_limit: Optional[int] = None,
    return_random_when_all_errors: bool = True,
) -> Dict[str, Any]:
    """Pure voting over precomputed execution records (used by eval scripts)."""

    ordered_records = sorted(records, key=lambda rec: rec.get("local_index", 0))
    for record in ordered_records:
        record.setdefault("result_vote_score", 0.0)
    signature_counter: Counter[Any] = Counter()
    for record in ordered_records:
        if record.get("success") and record.get("rows_signature") is not None:
            signature_counter[record["rows_signature"]] += 1

    triple_counter: Counter[Tuple[int, str, Any]] = Counter()
    triple_limit = triple_vote_limit or len(ordered_records)
    for record in ordered_records[: triple_limit]:
        if record.get("success"):
            triple_counter.update(record.get("triples") or [])

    winner_local_index: Optional[int] = None
    winning_score = 0.0
    best_key: Optional[Tuple[Any, ...]] = None

    maj_scores = []

    for record in ordered_records:
        if not record.get("success"):
            maj_scores.append([0.])
            continue
        idx = record.get("local_index")
        weight = 1.0
        if (
            voting_weights is not None
            and isinstance(idx, int)
            and 0 <= idx < len(voting_weights)
        ):
            weight = voting_weights[idx]

        triples = record.get("triples") or []
        triple_score = (
            sum(triple_counter[triple] for triple in triples if triple[2] is not None) / len(triples)
            if triples
            else 0.0
        )
        signature_score = signature_counter.get(record.get("rows_signature"), 0)
        score = (signature_score + triple_score) * weight
        maj_scores.append(score)

        ranking_key = (
            score,
            record.get("row_count", 0),
            record.get("vote_count", 0) or 0,
            -record.get("candidate_index", idx or 0),
        )

        if best_key is None or ranking_key > best_key:
            best_key = ranking_key
            winner_local_index = idx
            winning_score = score
        record["result_vote_score"] = score

    if winner_local_index is None and ordered_records and return_random_when_all_errors:
        fallback = random.choice(ordered_records)
        winner_local_index = fallback.get("local_index")
        winning_score = 0.0

    return {
        "records": ordered_records,
        "triple_counter": triple_counter,
        "result_signature_counter": signature_counter,
        "winner_local_index": winner_local_index,
        "winning_score": winning_score,
        "maj_scores": maj_scores,
    }


def run_result_voting(
    candidates: List[Dict[str, Any]],
    database_path: str,
    timeout_seconds: int,
    question_id: Any | None = None,
    return_random_when_all_errors: bool = True,
    max_workers: int = 1,
) -> Optional[Dict[str, Any]]:
    """Execute each valid SQL candidate and perform majority voting on result signatures."""

    if not database_path:
        logger.warning(
            "Missing database path question_id=%s",
            question_id,
        )
        return None

    records: List[ExecutionRecord] = []

    def _process_candidate(local_idx: int, candidate: Dict[str, Any]) -> ExecutionRecord:
        sql_text = (candidate.get("sql") or "").strip()
        record: ExecutionRecord = {
            "local_index": local_idx,
            "candidate_index": candidate.get("index", local_idx),
            "sql": sql_text,
            "success": False,
            "error": None,
            "row_count": 0,
            "triples": [],
        }

        if not sql_text:
            record["error"] = "EMPTY_SQL"
            return record

        if candidate.get("errors"):
            record["error"] = "; ".join(candidate.get("errors", [])) or "VALIDATION_ERRORS"
            return record

        outcome = _execute_with_timeout(
            sql_text=sql_text,
            database_path=database_path,
            timeout_seconds=timeout_seconds,
        )
        record.update(outcome)
        return record

    worker_count = max(1, max_workers)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(_process_candidate, idx, cand): idx
            for idx, cand in enumerate(candidates)
        }
        for future in as_completed(future_map):
            try:
                records.append(future.result())
            except Exception as exc:  # pragma: no cover - worker failure
                idx = future_map[future]
                logger.error(f"Candidate #{idx} failed: {exc}")
                records.append(
                    {
                        "local_index": idx,
                        "candidate_index": candidates[idx].get("index", idx),
                        "sql": candidates[idx].get("sql", ""),
                        "success": False,
                        "error": str(exc),
                        "row_count": 0,
                        "triples": [],
                    }
                )

    records.sort(key=lambda rec: rec["local_index"])

    triple_counter: Counter[Tuple[int, str, Any]] = Counter()
    for record in records:
        if record["success"]:
            triple_counter.update(record.get("triples", []))

    winner_local_index: Optional[int] = None
    winning_score = 0.0

    score_per_candidate: Dict[int, float] = {}
    for record in records:
        if not record["success"]:
            continue
        triples = record.get("triples", [])
        score = (
            sum(triple_counter[triple] for triple in triples) / len(triples)
            if triples
            else 0.0
        )
        score_per_candidate[record["local_index"]] = score

    if score_per_candidate:
        def _ranking_key(idx: int) -> Tuple[float, int, int, int, int]:
            candidate = candidates[idx]
            return (
                score_per_candidate[idx],
                candidate.get("vote_count", 0),
                1 if candidate.get("is_valid") else 0,
                -len(candidate.get("errors", [])),
                -candidate.get("index", idx),
            )

        winner_local_index = max(score_per_candidate.keys(), key=_ranking_key)
        winning_score = score_per_candidate[winner_local_index]

    if winner_local_index is None and records and return_random_when_all_errors:
        fallback_record = random.choice(records)
        winner_local_index = fallback_record["local_index"]
        winning_score = 0.0
        logger.warning(
            "All candidates failed; randomly choosing #%d",
            winner_local_index,
        )

    for record in records:
        candidate = candidates[record["local_index"]]
        candidate["execution_triples"] = record.get("triples", [])
        candidate["result_vote_score"] = score_per_candidate.get(record["local_index"], 0.0)
        candidate["execution_success"] = record["success"]
        candidate["execution_error"] = record["error"]
        candidate["execution_row_count"] = record.get("row_count", 0)

    total_success = sum(rec["success"] for rec in records)
    logger.info(
        f"Voting question_id={question_id}: success={total_success}/{len(records)}, score={winning_score:.2f}"
    )

    return {
        "records": records,
        "triple_counter": triple_counter,
        "winner_local_index": winner_local_index,
        "winning_score": winning_score,
    }


def normalize_sql(sql: str | None) -> str:
    return (sql or "").strip()


def canonicalize_rows(
    rows: Sequence[Sequence[object]] | None,
) -> frozenset[Tuple[object, ...]]:
    canonical_rows: List[Tuple[object, ...]] = []
    if rows is None:
        return frozenset()
    for row in rows:
        normalized = []
        for value in row:
            if isinstance(value, memoryview):
                value = value.tobytes()
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            normalized.append(value)
        canonical_rows.append(tuple(normalized))
    return frozenset(canonical_rows)


def rows_to_triples(
    rows: frozenset[Tuple[object, ...]] | None,
) -> List[Tuple[int, str, object]]:
    if rows is None:
        return []
    triples: List[Tuple[int, str, object]] = []
    for row_idx, row in enumerate(rows):
        for col_idx, value in enumerate(row):
            triples.append((row_idx, f"col_{col_idx}", value))
    return triples


def _lookup_job_result(
    candidate: Dict[str, Any],
    job_results: Sequence[Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    candidate_index = candidate.get("index")
    if (
        isinstance(candidate_index, int)
        and 0 <= candidate_index < len(job_results)
    ):
        return job_results[candidate_index]
    return None


def run_cached_voting(
    *,
    candidates: List[Dict[str, Any]],
    job_results: Sequence[Optional[Dict[str, Any]]],
    voting_weights: Sequence[float] | None = None,
    triple_vote_limit: int | None = None,
    return_random_one_when_all_errors: bool = True,
) -> Dict[str, Any]:
    """Reuse cached execution results for selection voting."""

    records: List[Dict[str, Any]] = []

    for local_idx, candidate in enumerate(candidates):
        sql_text = normalize_sql(candidate.get("sql"))
        exec_result = candidate.get("exec_result") or _lookup_job_result(
            candidate, job_results
        )

        record = {
            "local_index": candidate.get("index", local_idx),
            "candidate_index": candidate.get("index", local_idx),
            "sql": sql_text,
            "success": False,
            "error": None,
            "row_count": 0,
            "triples": [],
            "rows_signature": None,
            "vote_score": 0.0,
            "vote_count": candidate.get("vote_count", 0) or 0,
        }

        if not sql_text:
            record["error"] = "EMPTY_SQL"
        elif not exec_result:
            record["error"] = "MISSING_EXECUTION_RESULT"
        elif exec_result.get("error"):
            record["error"] = exec_result.get("error")
        elif not exec_result.get("success"):
            record["error"] = exec_result.get("error") or "EXECUTION_FAILED"
        else:
            rows = exec_result.get("rows")
            if not rows:
                rows = canonicalize_rows(exec_result.get("result"))
            record["success"] = True
            raw_rows = exec_result.get("result") or []
            record["row_count"] = len(raw_rows)
            record["rows_signature"] = rows
            record["triples"] = rows_to_triples(rows)

        records.append(record)

    summary = weighted_majority_vote(
        records,
        voting_weights=voting_weights,
        triple_vote_limit=triple_vote_limit,
        return_random_when_all_errors=return_random_one_when_all_errors,
    )

    signature_counter: Counter[Any] = summary.get("result_signature_counter", Counter())
    total_success = sum(signature_counter.values()) or 1
    for record in records:
        sig = record.get("rows_signature")
        if sig is None:
            record["vote_score"] = 0.0
        else:
            record["vote_score"] = signature_counter.get(sig, 0) / total_success

    return {
        "records": records,
        "triple_counter": summary.get("triple_counter", Counter()),
        "winner_local_index": summary.get("winner_local_index"),
        "winning_score": summary.get("winning_score", 0.0),
        "maj_scores": summary.get("maj_scores", []),
    }


def _execute_with_timeout(
    sql_text: str,
    database_path: str,
    timeout_seconds: int,
) -> ExecutionRecord:
    """Execute SQL with rollback and timeout, returning result triples."""

    def _run_query() -> Tuple[List[Tuple[Any, ...]], List[str], List[Tuple[int, str, Any]]]:
        conn = sqlite3.connect(database_path)
        try:
            conn.execute("BEGIN TRANSACTION;")
            cursor = conn.cursor()
            cursor.execute(sql_text)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            triples: List[Tuple[int, str, Any]] = []
            if columns:
                for row_idx, row in enumerate(rows):
                    for col_idx, value in enumerate(row):
                        column_name = columns[col_idx] if col_idx < len(columns) else f"col_{col_idx}"
                        triples.append((row_idx, column_name, value))
            conn.rollback()
            return rows, columns, triples
        finally:
            conn.close()

    try:
        if timeout_seconds and timeout_seconds > 0:
            rows, columns, triples = func_timeout(timeout_seconds, _run_query)
        else:
            rows, columns, triples = _run_query()
        return {
            "success": True,
            "error": None,
            "row_count": len(rows),
            "columns": columns,
            "triples": triples,
        }
    except FunctionTimedOut:
        return {
            "success": False,
            "error": f"timeout>{timeout_seconds}s",
            "row_count": 0,
            "columns": [],
            "triples": [],
        }
    except Exception as exc:  # pragma: no cover - sqlite failure
        return {
            "success": False,
            "error": str(exc),
            "row_count": 0,
            "columns": [],
            "triples": [],
        }
