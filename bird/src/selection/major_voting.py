"""Optimized version of eval_pass_at_k.py that uses cached results for voting."""
###
from __future__ import annotations

import random
import sqlite3
import time
import re
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple, Optional
from collections import Counter


def _connect_sqlite_readonly(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def serialize_value(value):
    if isinstance(value, (int, float, str)) or value is None:
        return value
    return str(value)


def run_query(db_file, sql, max_chars: int | None = None):
    if "iif" in sql.lower():
        sql = iif_to_case_nested(sql)

    result = {
        "rows": None,
        "columns": [],
        "error": None,
        "triples": []
    }

    if sql is None:
        result["error"] = "Empty SQL"
        return result

    stripped_sql = sql.strip()
    if stripped_sql == "":
        result["error"] = "Empty SQL"
        return result

    if stripped_sql.lower() == "error sql":
        result["error"] = "Marked as invalid SQL"
        return result

    conn = _connect_sqlite_readonly(db_file)
    try:
        conn.execute("BEGIN TRANSACTION;")
        cursor = conn.cursor()
        cursor.execute(sql)
        if max_chars is None or max_chars <= 0:
            rows = cursor.fetchall()
        else:
            rows = []
            char_count = 0
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            while True:
                batch = cursor.fetchmany(128)
                if not batch:
                    break
                stop = False
                for row in batch:
                    serialized = [serialize_value(v) for v in row]
                    row_text = "\t".join("" if v is None else str(v) for v in serialized) + "\n"
                    char_count += len(row_text)
                    if char_count > max_chars:
                        stop = True
                        break
                    rows.append(serialized)
                if stop:
                    break
            result["rows"] = rows
            result["columns"] = columns
            triples = []
            for row_idx, row in enumerate(rows):
                for col_idx, value in enumerate(row):
                    triples.append((row_idx, columns[col_idx], value))
            result["triples"] = triples
            return result
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        result["rows"] = [[serialize_value(v) for v in row] for row in rows]
        result["columns"] = columns

        triples = []
        for row_idx, row in enumerate(rows):
            for col_idx, value in enumerate(row):
                triples.append((row_idx, columns[col_idx], value))
        result["triples"] = triples
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        conn.rollback()
        conn.close()

    return result


def compare_sql(db_file, question, ground_truth, pred_sql, max_result_chars: int | None = None):
    pred_info = run_query(db_file, pred_sql, max_chars=max_result_chars)
    gold_info = run_query(db_file, ground_truth, max_chars=max_result_chars)

    correctness = 0
    if pred_info["error"] is None and gold_info["error"] is None:
        pred_rows = pred_info["rows"]
        gold_rows = gold_info["rows"]
        pred_set = {tuple(row) for row in pred_rows}
        gold_set = {tuple(row) for row in gold_rows}
        if pred_set == gold_set:# and len(pred_rows) == len(gold_rows):
            correctness = 1

    result = {
        "db_file": db_file,
        "question": question,
        "gold_sql": ground_truth,
        "gold_result": gold_info["rows"],
        "gold_columns": gold_info["columns"],
        "gold_execution_error": gold_info["error"],
        "pred_sql": pred_sql,
        "pred_result": pred_info["rows"],
        "pred_columns": pred_info["columns"],
        "pred_execution_error": pred_info["error"],
        "correctness": correctness
    }

    return result


def iif_to_case_nested(sql: str) -> str:
    """Rewrite SQL Server `IIF(condition, true_val, false_val)` into
    `CASE WHEN ... END` expressions, handling nested IIF calls.
    """
    # Strategy: repeatedly rewrite the innermost IIF (no nested IIF inside).
    # Matching rules:
    # - `\bIIF\b` matches the keyword.
    # - `\(` matches the opening parenthesis (balanced matching).

    def replace_innermost_iif(text):
        # Locate all IIF occurrences.
        iif_pattern = re.compile(r'\bIIF\s*\(', re.IGNORECASE)
        matches = list(iif_pattern.finditer(text))
        if not matches:
            return text, False

        # Scan left-to-right, tracking balanced parentheses.
        # Nested IIF: skip the outer one and continue after it.
        pos = 0
        while pos < len(text):
            match = iif_pattern.search(text, pos)
            if not match:
                break
            start = match.start()
            # Find the matching closing paren of this IIF.
            # The opening paren is the char just before match.end().
            paren_start = match.end() - 1  # position of '('
            paren_count = 1
            i = paren_start + 1
            while i < len(text) and paren_count > 0:
                if text[i] == '(':
                    paren_count += 1
                elif text[i] == ')':
                    paren_count -= 1
                i += 1
            if paren_count != 0:
                raise ValueError("Unmatched parentheses in SQL")

            iif_full_end = i  # position after the closing ')'
            iif_content = text[paren_start+1:iif_full_end-1]  # inner content

            # If the content contains another IIF, handle the inner one first.
            if re.search(r'\bIIF\b', iif_content, re.IGNORECASE):
                # Skip the outer IIF and try the inner one.
                pos = match.end()
                continue

            # Found an innermost IIF: split its arguments.
            args = _split_iif_args(iif_content)
            if len(args) != 3:
                raise ValueError(f"Invalid IIF argument count: {iif_content}")

            cond, true_val, false_val = args
            case_expr = f"(CASE WHEN {cond} THEN {true_val} ELSE {false_val} END)"
            new_text = text[:start] + case_expr + text[iif_full_end:]
            return new_text, True

        return text, False

    # Iteratively rewrite until no IIF remains.
    current = sql
    while True:
        current, replaced = replace_innermost_iif(current)
        if not replaced:
            break
    return current


def _split_iif_args(content: str):
    """Split IIF arguments on commas, respecting quotes and parentheses.
    """
    args = []
    current = ""
    in_single_quote = False
    in_double_quote = False
    paren_depth = 0
    i = 0
    while i < len(content):
        c = content[i]
        if c == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif c == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif c == '(' and not in_single_quote and not in_double_quote:
            paren_depth += 1
        elif c == ')' and not in_single_quote and not in_double_quote:
            paren_depth -= 1
        elif c == ',' and paren_depth == 0 and not in_single_quote and not in_double_quote:
            args.append(current.strip())
            current = ""
            i += 1
            continue

        current += c
        i += 1
    if current.strip():
        args.append(current.strip())
    return args


def weighted_majority_vote(
    records: List[Dict[str, Any]],
    voting_weights: Sequence[float] | None = None,
    triple_vote_limit: Optional[int] = None,
    return_random_when_all_errors: bool = True,
) -> Dict[str, Any]:
    """Pure voting over precomputed execution records (used by eval scripts)."""

    ordered_records = sorted(records, key=lambda rec: rec.get("local_index", 0))
    signature_counter: Counter[Any] = Counter()
    for record in ordered_records:
        if record.get("success") and record.get("rows_signature") is not None:
            signature_counter[record["rows_signature"]] += 1

    triple_counter: Counter[Tuple[int, str, Any]] = Counter()
    triple_limit = triple_vote_limit or len(ordered_records)
    for idx, record in enumerate(ordered_records[: triple_limit]):
        if record.get("success"):
            weight = voting_weights[idx]

            for i in range(weight):
                triple_counter.update(record.get("triples") or [])
    # print(triple_counter)

    winner_local_index: Optional[int] = None
    winning_score = 0.0
    best_key: Optional[Tuple[Any, ...]] = None
    maj_scores = []

    for record in ordered_records:
        if not record.get("success"):
            maj_scores.append(-5)
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
        # score = triple_score
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

    if winner_local_index is None and ordered_records and return_random_when_all_errors:
        fallback = random.choice(ordered_records)
        winner_local_index = fallback.get("local_index")
        winning_score = 0.0

    # from IPython import embed; embed()

    return {
        "maj_scores": maj_scores,
        # "records": ordered_records,
        "triple_counter": triple_counter,
        "result_signature_counter": signature_counter,
        "winner_local_index": winner_local_index,
        "winning_score": winning_score,
    }


def normalize_sql(sql: str | None) -> str:
    return (sql or "").strip()


def canonicalize_rows(rows: Sequence[Sequence[object]] | None) -> frozenset[Tuple[object, ...]]:
    canonical_rows: list[Tuple[object, ...]] = []
    if rows is None:
        return frozenset()
    for row in rows:
        canonical_row = []
        for value in row:
            if isinstance(value, memoryview):
                value = value.tobytes()
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            canonical_row.append(value)
        canonical_rows.append(tuple(canonical_row))
    return frozenset(canonical_rows)


def rows_to_triples(rows: frozenset[Tuple[object, ...]] | None) -> list[Tuple[int, str, object]]:
    """Convert canonicalized rows to triples format used by voting."""
    if rows is None:
        return []

    triples = []
    for row_idx, row in enumerate(rows):
        for col_idx, value in enumerate(row):
            column_name = f"col_{col_idx}"
            triples.append((row_idx, column_name, value))
    return triples


def run_cached_voting(
    candidates: list[dict],
    job_results: dict[tuple[str, str], tuple[frozenset[Tuple[object, ...]] | None, str | None]],
    db_id: str,
    *,
    voting_weights: Sequence[float] | None = None,
    triple_vote_limit: int | None = None,
    return_random_one_when_all_errors: bool = True,
) -> dict | None:
    """Run majority voting using pre-computed SQL results."""

    records = []

    for local_idx, candidate in enumerate(candidates):
        sql_text = normalize_sql(candidate.get("sql"))
        record = {
            "local_index": local_idx,
            "candidate_index": candidate.get("index", local_idx),
            "sql": sql_text,
            "success": False,
            "error": None,
            "row_count": 0,
            "triples": [],
            "rows_signature": None,
        }

        if not sql_text:
            record["error"] = "EMPTY_SQL"
        elif candidate.get("errors"):
            record["error"] = "; ".join(candidate.get("errors", [])) or "VALIDATION_ERRORS"
        else:
            rows, error = job_results.get((db_id, sql_text), (None, None))
            if error:
                record["error"] = error
            elif rows is not None:
                record["success"] = True
                record["row_count"] = len(rows)
                record["triples"] = rows_to_triples(rows)
                record["rows_signature"] = rows

        records.append(record)

    summary = weighted_majority_vote(
        records,
        voting_weights=voting_weights,
        triple_vote_limit=triple_vote_limit,
        return_random_when_all_errors=return_random_one_when_all_errors,
    )

    signature_counter: Counter[Any] = summary.get("result_signature_counter", Counter())
    freq_local_index: int | None = None
    freq_score = 0.0
    valid_indices = [record["local_index"] for record in records if record.get("success")]
    total_success = sum(signature_counter.values())
    if valid_indices:
        def _freq_key(idx: int) -> Tuple[int, int, int]:
            record = records[idx]
            sig = record.get("rows_signature")
            count = signature_counter.get(sig, 0)
            vote_count = candidates[idx].get("vote_count", 0) or 0
            candidate_index = candidates[idx].get("index", idx)
            return (
                count,
                vote_count,
                -candidate_index,
            )

        freq_local_index = max(valid_indices, key=_freq_key)
        freq_sig = records[freq_local_index].get("rows_signature")
        if total_success:
            freq_score = signature_counter.get(freq_sig, 0) / total_success

    if freq_local_index is None and summary.get("winner_local_index") is not None:
        freq_local_index = summary.get("winner_local_index")
        freq_score = 0.0

    return {
        "records": records,
        "maj_scores": summary.get("maj_scores", []),
        "triple_counter": summary.get("triple_counter", Counter()),
        "winner_local_index": summary.get("winner_local_index"),
        "winning_score": summary.get("winning_score", 0.0),
        "freq_local_index": freq_local_index,
        "freq_score": freq_score,
        "null_value_count": summary.get("null_value_count", 0),
    }


def resolve_db_path(db_root: Path, db_id: str) -> Path:
    db_path = db_root / db_id / f"{db_id}.sqlite"
    if not db_path.exists():
        raise FileNotFoundError(f"Missing SQLite file for {db_id}: {db_path}")
    return db_path


def prepare_candidates(
    sample: dict,
    limit: int | None,
    exclude_auto_correct: bool,
    source_models: set[str] | None,
) -> list[dict]:
    candidates = sorted(sample.get("sql_candidates") or [], key=lambda item: item.get("index", 0))
    if source_models:
        candidates = [
            cand
            for cand in candidates
            if str(cand.get("source_model", "")).lower() in source_models
        ]
    if exclude_auto_correct:
        candidates = [
            cand for cand in candidates if cand.get("auto_correct_attempts", 0) != 1
        ]
    if limit is not None:
        candidates = candidates[:limit]
    return candidates


def _run_single_sql(db_path: str, sql: str, timeout: int) -> frozenset[Tuple[object, ...]]:
    conn = _connect_sqlite_readonly(db_path)
    start_time = time.time()

    def _progress_handler() -> None:
        if timeout > 0 and time.time() - start_time > timeout:
            raise TimeoutError("SQL execution timed out")

    try:
        if timeout > 0:
            conn.set_progress_handler(_progress_handler, 1000)
        else:
            conn.set_progress_handler(None, 0)
        conn.execute("BEGIN TRANSACTION;")
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.rollback()
        return canonicalize_rows(rows)
        # return
    finally:
        conn.set_progress_handler(None, 0)
        conn.close()


def _execute_sql_job(job: tuple[str, str, str, int]) -> tuple[str, str, frozenset[Tuple[object, ...]] | None, str | None]:
    db_path, db_id, sql, timeout = job
    try:
        if "iif" in sql.lower():
            sql = iif_to_case_nested(sql)
        rows = _run_single_sql(db_path, sql, timeout)
        return db_id, sql, rows, None
    except TimeoutError:
        return db_id, sql, None, "Timeout"
    except Exception as exc:  # pylint: disable=broad-except
        return db_id, sql, None, str(exc)
