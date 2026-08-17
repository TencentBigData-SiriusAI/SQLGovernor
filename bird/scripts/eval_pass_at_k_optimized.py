"""Optimized version of eval_pass_at_k.py that uses cached results for voting."""
###
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import random
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, List, Sequence, Tuple
from collections import Counter
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import Settings
from src.tools.sql_result_voter import weighted_majority_vote


LEGACY_DATA_PREFIX = Path("data")
NEW_DATA_PREFIX = Path("data")


class SQLJobExecutor:
    """Helper to execute SQL jobs incrementally, optionally with multiprocessing."""

    def __init__(self, workers: int, timeout: int):
        self.timeout = timeout
        self.workers = max(1, workers)
        self.pool: mp.pool.Pool | None = None
        if self.workers > 1:
            self.pool = mp.Pool(processes=self.workers)

    def run(
        self, jobs: list[tuple[str, str, str, int]]
    ) -> list[tuple[str, str, frozenset[Tuple[object, ...]] | None, str | None]]:
        if not jobs:
            return []
        if self.pool is None:
            return [_execute_sql_job(job) for job in jobs]
        chunk = max(1, len(jobs) // (self.workers * 2))
        return list(self.pool.imap_unordered(_execute_sql_job, jobs, chunksize=chunk))

    def close(self) -> None:
        if self.pool is None:
            return
        self.pool.close()
        self.pool.join()



def parse_k_values(values: Iterable[str]) -> List[int]:
    tokens: List[int] = []
    for value in values:
        for chunk in value.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            k = int(chunk)
            if k <= 0:
                raise ValueError("k must be a positive integer")
            tokens.append(k)
    if not tokens:
        raise ValueError("Provide at least one k value")
    return sorted(set(tokens))


def parse_weight_values(value: str | None) -> List[float]:
    if not value:
        return []
    weights: List[float] = []
    for chunk in value.replace(";", ",").split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            weights.append(float(token))
        except ValueError as exc:  # pragma: no cover - CLI validation
            raise ValueError(f"Invalid weight value: {token}") from exc
    return weights


def normalize_sql(sql: str | None) -> str:
    return (sql or "").strip()


def normalize_difficulty(value: str | None) -> str:
    if value is None:
        return "unknown"
    normalized = str(value).strip().lower()
    return normalized or "unknown"


def sort_difficulty_keys(keys: Iterable[str]) -> list[str]:
    preferred = ["simple", "moderate", "challenging"]
    key_set = {key for key in keys if key}
    ordered = [key for key in preferred if key in key_set]
    ordered.extend(sorted(key_set - set(preferred)))
    return ordered


def _ensure_difficulty_metrics(
    metrics: dict[str, dict],
    difficulty: str,
    ks: Sequence[int],
) -> None:
    totals: dict[str, int] = metrics["totals"]
    pass_counts: dict[str, dict[int, int]] = metrics["pass_counts"]
    major_evaluated: dict[str, int] = metrics["major_evaluated"]
    major_correct: dict[str, int] = metrics["major_correct"]
    if difficulty not in totals:
        totals[difficulty] = 0
        pass_counts[difficulty] = {k: 0 for k in ks}
        major_evaluated[difficulty] = 0
        major_correct[difficulty] = 0


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


def is_correct(
    pred: frozenset[Tuple[object, ...]] | None,
    gt: frozenset[Tuple[object, ...]] | None,
) -> bool:
    if pred is None and gt is None:
        return True
    if pred is None or gt is None:
        return False
    return pred == gt


def major_voting(
    triples: list[list[Tuple[int, str, object]]],
    valid: list[bool],
) -> tuple[int, list[float] | None]:
    all_sets = triples
    freq_idx = 0
    if sum(valid) == 0:
        freq_idx = random.choice(range(len(all_sets)))
        return freq_idx, None

    triple_counts: dict[Tuple[int, str, object], int] = {}
    for res, val in zip(all_sets, valid):
        if val:
            for triple in res:
                triple_counts[triple] = triple_counts.get(triple, 0) + 1

    result_scores: list[float] = []
    for res, val in zip(all_sets, valid):
        if val:
            score = (
                sum(triple_counts[triple] for triple in res if triple[2] is not None) / len(res)
                if res
                else 0.0
            )
            result_scores.append(score)
        else:
            result_scores.append(0.0)

    if result_scores:
        freq_idx = int(max(range(len(result_scores)), key=result_scores.__getitem__))
    else:
        return 0, None
    return freq_idx, result_scores


def get_frequency_and_group_id(
    triple_list: list[list[Tuple[int, str, object]]],
    result_list: list[frozenset[Tuple[object, ...]] | None],
    valid_list: list[bool],
) -> tuple[int, list[float] | None, list[float], list[int]]:
    normalized_list = [tuple(x) if x is not None else None for x in result_list]
    counter = Counter(normalized_list)
    total_length = len(result_list)

    group_map: dict[Tuple[object, ...] | None, int] = {}
    current_group_id = 0
    group_id_list: list[int] = []
    freq_score: list[float] = []

    for x_normalized in normalized_list:
        if x_normalized is None:
            freq_score.append(0.0)
            group_id_list.append(-1)
        else:
            freq_score.append(counter[x_normalized] / total_length if total_length else 0.0)
            if x_normalized not in group_map:
                group_map[x_normalized] = current_group_id
                current_group_id += 1
            group_id_list.append(group_map[x_normalized])

    maj_idx, maj_score = major_voting(triple_list, valid_list)
    return maj_idx, maj_score, freq_score, group_id_list


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
            "vote_count": candidate.get("vote_count", 0) or 0,
            "result_vote_score": 0.0,
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
        "triple_counter": summary.get("triple_counter", Counter()),
        "winner_local_index": summary.get("winner_local_index"),
        "winning_score": summary.get("winning_score", 0.0),
        "freq_local_index": freq_local_index,
        "freq_score": freq_score,
        "null_value_count": summary.get("null_value_count", 0),
    }


def _build_vote_score_entry(
    *,
    question_id: int | str | None,
    db_id: str | None,
    summary: dict | None,
) -> dict | None:
    if summary is None:
        return None

    records = summary.get("records") or []
    entry = {
        "question_id": question_id,
        "db_id": db_id,
        "winner_local_index": summary.get("winner_local_index"),
        "winning_score": summary.get("winning_score", 0.0),
        "freq_local_index": summary.get("freq_local_index"),
        "freq_score": summary.get("freq_score", 0.0),
        "null_value_count": summary.get("null_value_count", 0),
        "candidates": [],
    }

    for record in records:
        entry["candidates"].append(
            {
                "local_index": record.get("local_index"),
                "candidate_index": record.get("candidate_index"),
                "vote_count": record.get("vote_count"),
                "result_vote_score": record.get("result_vote_score", 0.0),
                "success": record.get("success"),
                "row_count": record.get("row_count"),
                "error": record.get("error"),
                "sql": record.get("sql"),
            }
        )

    return entry


def load_experiment(path: Path) -> tuple[dict, list[dict]]:
    payload = json.loads(path.read_text())
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("Input JSON missing 'results' list")
    return payload, results


def _remap_legacy_path(path: Path) -> Path:
    try:
        relative = path.relative_to(LEGACY_DATA_PREFIX)
    except ValueError:
        return path
    return NEW_DATA_PREFIX / relative


def parse_source_models(value: str | None) -> set[str] | None:
    if value is None:
        return None
    value = value.strip()
    if not value or value.lower() == "all":
        return None
    tokens = {chunk.strip().lower() for chunk in value.replace(",", " ").split() if chunk.strip()}
    return tokens or None


def load_gold_sql_map(path: Path) -> dict[int, dict[str, str | None]]:
    entries = json.loads(path.read_text())
    if not isinstance(entries, list):
        raise ValueError(f"Gold file {path} must contain a list of entries")
    mapping: dict[int, dict[str, str | None]] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        question_id = item.get("question_id")
        db_id = item.get("db_id")
        sql = normalize_sql(item.get("SQL") or item.get("sql"))
        difficulty = item.get("difficulty")
        if question_id is None or not db_id or not sql:
            continue
        mapping[int(question_id)] = {"db_id": db_id, "sql": sql, "difficulty": difficulty}
    if not mapping:
        raise ValueError(f"Gold file {path} did not yield any usable SQLs")
    return mapping


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


def build_job_map(
    results: list[dict],
    candidate_pool_size: int | None,
    db_root: Path,
    exclude_auto_correct: bool,
    source_models: set[str] | None,
    gold_sql_map: dict[int, dict[str, str | None]] | None,
) -> dict[tuple[str, str], Path]:
    job_map: dict[tuple[str, str], Path] = {}
    limit = candidate_pool_size if candidate_pool_size and candidate_pool_size > 0 else None
    for sample in results:
        db_id = sample.get("db_id")
        if not db_id:
            continue
        db_path = resolve_db_path(db_root, db_id)

        candidates = prepare_candidates(sample, limit, exclude_auto_correct, source_models)
        for candidate in candidates:
            sql = normalize_sql(candidate.get("sql"))
            if not sql:
                continue
            job_map.setdefault((db_id, sql), db_path)

        if gold_sql_map is not None:
            question_id = sample.get("question_id")
            if question_id is None:
                continue
            gold_entry = gold_sql_map.get(int(question_id))
            if not gold_entry:
                continue
            gold_db_id = gold_entry.get("db_id")
            gold_sql = normalize_sql(gold_entry.get("sql"))
            if not gold_sql:
                continue
            if not gold_db_id:
                continue
            gold_db_path = resolve_db_path(db_root, gold_db_id)
            job_map.setdefault((gold_db_id, gold_sql), gold_db_path)
    return job_map


def _run_single_sql(db_path: str, sql: str, timeout: int) -> frozenset[Tuple[object, ...]]:
    conn = sqlite3.connect(db_path)
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
    finally:
        conn.set_progress_handler(None, 0)
        conn.close()


def _execute_sql_job(job: tuple[str, str, str, int]) -> tuple[str, str, frozenset[Tuple[object, ...]] | None, str | None]:
    db_path, db_id, sql, timeout = job
    try:
        rows = _run_single_sql(db_path, sql, timeout)
        return db_id, sql, rows, None
    except TimeoutError:
        return db_id, sql, None, "Timeout"
    except Exception as exc:  # pylint: disable=broad-except
        return db_id, sql, None, str(exc)


def execute_jobs(
    job_map: dict[tuple[str, str], Path], workers: int, timeout: int
) -> dict[tuple[str, str], tuple[frozenset[Tuple[object, ...]] | None, str | None]]:
    if not job_map:
        return {}
    jobs = [(str(path), db_id, sql, timeout) for (db_id, sql), path in job_map.items()]
    results: dict[tuple[str, str], tuple[frozenset[Tuple[object, ...]] | None, str | None]] = {}
    with mp.Pool(processes=workers) as pool:
        for db_id, sql, rows, error in pool.imap_unordered(_execute_sql_job, jobs, chunksize=8):
            results[(db_id, sql)] = (rows, error)
    return results


def evaluate_samples(
    samples: list[dict],
    ks: list[int],
    job_results: dict[tuple[str, str], tuple[frozenset[Tuple[object, ...]] | None, str | None]],
    rng: random.Random | None,
    candidate_pool_size: int | None,
    shuffle_candidates: bool,
    exclude_auto_correct: bool,
    source_models: set[str] | None,
    gold_sql_map: dict[int, dict[str, str | None]],
    db_root: Path,
    voting_weights: Sequence[float] | None,
    triple_vote_limit: int | None,
) -> tuple[
    dict[int, int],
    int,
    int,
    int,
    list[dict],
    list[dict],
    list[dict],
    list[dict],
    dict[str, dict],
]:
    max_k = max(ks)
    counts = {k: 0 for k in ks}
    total = 0
    major_evaluated = 0
    major_correct = 0
    major_failures: list[dict] = []
    pass_failures: list[dict] = []
    vote_score_entries: list[dict] = []
    major_detail_entries: list[dict] = []
    limit = candidate_pool_size if candidate_pool_size and candidate_pool_size > 0 else None
    difficulty_metrics: dict[str, dict] = {
        "totals": {},
        "pass_counts": {},
        "major_evaluated": {},
        "major_correct": {},
    }
    difficulty_totals: dict[str, int] = difficulty_metrics["totals"]
    difficulty_pass_counts: dict[str, dict[int, int]] = difficulty_metrics["pass_counts"]
    difficulty_major_evaluated: dict[str, int] = difficulty_metrics["major_evaluated"]
    difficulty_major_correct: dict[str, int] = difficulty_metrics["major_correct"]

    for i, sample in enumerate(samples):
        question_id = sample.get("question_id")
        db_id = sample.get("db_id")
        if question_id is None or not db_id:
            continue
        gold_entry = gold_sql_map.get(int(question_id))
        if not gold_entry:
            continue
        gold_db_id = gold_entry.get("db_id")
        gold_sql = normalize_sql(gold_entry.get("sql"))
        if not gold_db_id or not gold_sql:
            continue
        gold_rows, _ = job_results.get((gold_db_id, gold_sql), (None, None))
        difficulty = normalize_difficulty(
            gold_entry.get("difficulty") or sample.get("difficulty")
        )

        candidates = prepare_candidates(sample, limit, exclude_auto_correct, source_models)
        if not candidates:
            continue

        total += 1
        _ensure_difficulty_metrics(difficulty_metrics, difficulty, ks)
        difficulty_totals[difficulty] += 1

        # Progress reporting every 50 samples
        if (i + 1) % 50 == 0 or i + 1 == len(samples):
            print(f"Progress: {i + 1}/{len(samples)} samples processed ({(i + 1)/len(samples)*100:.1f}%)")
        indices = list(range(len(candidates)))
        if shuffle_candidates and rng is not None:
            rng.shuffle(indices)
        max_considered = min(len(indices), max_k)
        considered_candidates: list[dict] = []
        pass_correct_list: list[bool] = []
        for offset in range(max_considered):
            idx = indices[offset]
            candidate = candidates[idx]
            sql = normalize_sql(candidate.get("sql"))
            rows, _ = job_results.get((db_id, sql), (None, None))
            position = offset + 1
            considered_candidates.append(
                {
                    "position": position,
                    "candidate_index": candidate.get("index", idx),
                    "sql": sql,
                }
            )
            pass_correct_list.append(is_correct(rows, gold_rows))

        for k in ks:
            if any(pass_correct_list[: min(k, len(pass_correct_list))]):
                counts[k] += 1
                difficulty_pass_counts[difficulty][k] += 1

        if not pass_correct_list or not any(pass_correct_list):
            pass_failures.append(
                {
                    "question_id": question_id,
                    "db_id": db_id,
                    "max_k": max_k,
                    "considered": considered_candidates,
                    "candidate_pool_size": len(candidates),
                }
            )

        # Use cached results instead of re-executing SQL
        summary = run_cached_voting(
            candidates,
            job_results,
            db_id,
            voting_weights=voting_weights,
            triple_vote_limit=triple_vote_limit,
        )
        vote_entry = _build_vote_score_entry(
            question_id=question_id,
            db_id=db_id,
            summary=summary,
        )
        if vote_entry:
            vote_score_entries.append(vote_entry)
        records = summary.get("records", []) if summary else []
        triple_list = [record.get("triples") or [] for record in records]
        valid_list = [bool(record.get("success")) for record in records]
        result_list: list[frozenset[Tuple[object, ...]] | None] = []
        correct_list: list[bool] = []
        error_list: list[str | None] = []
        for candidate in candidates:
            sql = normalize_sql(candidate.get("sql"))
            rows, error = job_results.get((db_id, sql), (None, None))
            result_list.append(rows)
            correct_list.append(is_correct(rows, gold_rows))
            error_list.append(error)

        maj_idx, _maj_score, _freq_score, _group_id_list = get_frequency_and_group_id(
            triple_list,
            result_list,
            valid_list,
        )
        if candidates:
            major_evaluated += 1
            difficulty_major_evaluated[difficulty] += 1
            major_is_correct = correct_list[maj_idx]
            if major_is_correct:
                major_correct += 1
                difficulty_major_correct[difficulty] += 1
            pass_any = any(pass_correct_list)
            if not major_is_correct and pass_any:
                first_pass_idx = pass_correct_list.index(True)
                pass_candidate = considered_candidates[first_pass_idx]
                major_failures.append(
                    {
                        "question_id": question_id,
                        "db_id": db_id,
                        "major_sql": normalize_sql(candidates[maj_idx].get("sql")),
                        "pass_sql": pass_candidate.get("sql"),
                        "pass_position": pass_candidate.get("position"),
                        "candidate_index": pass_candidate.get("candidate_index"),
                    }
                )
            major_detail_entries.append(
                {
                    "question_id": question_id,
                    "db_id": db_id,
                    "winner_local_index": maj_idx,
                    "major_sql": normalize_sql(candidates[maj_idx].get("sql")),
                    "major_correct": major_is_correct,
                    "major_error": error_list[maj_idx],
                }
            )

    return (
        counts,
        total,
        major_evaluated,
        major_correct,
        major_failures,
        pass_failures,
        vote_score_entries,
        major_detail_entries,
        difficulty_metrics,
    )


def evaluate_samples_streaming(
    samples: list[dict],
    ks: list[int],
    rng: random.Random | None,
    candidate_pool_size: int | None,
    shuffle_candidates: bool,
    exclude_auto_correct: bool,
    source_models: set[str] | None,
    gold_sql_map: dict[int, dict[str, str | None]],
    db_root: Path,
    workers: int,
    timeout: int,
    voting_weights: Sequence[float] | None,
    triple_vote_limit: int | None,
) -> tuple[
    dict[int, int],
    int,
    int,
    int,
    list[dict],
    list[dict],
    list[dict],
    list[dict],
    dict[str, dict],
]:
    max_k = max(ks)
    counts = {k: 0 for k in ks}
    total = 0
    major_evaluated = 0
    major_correct = 0
    major_failures: list[dict] = []
    pass_failures: list[dict] = []
    vote_score_entries: list[dict] = []
    major_detail_entries: list[dict] = []
    limit = candidate_pool_size if candidate_pool_size and candidate_pool_size > 0 else None
    executor = SQLJobExecutor(workers, timeout)
    difficulty_metrics: dict[str, dict] = {
        "totals": {},
        "pass_counts": {},
        "major_evaluated": {},
        "major_correct": {},
    }
    difficulty_totals: dict[str, int] = difficulty_metrics["totals"]
    difficulty_pass_counts: dict[str, dict[int, int]] = difficulty_metrics["pass_counts"]
    difficulty_major_evaluated: dict[str, int] = difficulty_metrics["major_evaluated"]
    difficulty_major_correct: dict[str, int] = difficulty_metrics["major_correct"]

    try:
        for i, sample in enumerate(samples):
            question_id = sample.get("question_id")
            db_id = sample.get("db_id")
            if question_id is None or not db_id:
                continue
            gold_entry = gold_sql_map.get(int(question_id))
            if not gold_entry:
                continue
            gold_db_id = gold_entry.get("db_id")
            gold_sql = normalize_sql(gold_entry.get("sql"))
            if not gold_db_id or not gold_sql:
                continue
            difficulty = normalize_difficulty(
                gold_entry.get("difficulty") or sample.get("difficulty")
            )

            candidates = prepare_candidates(sample, limit, exclude_auto_correct, source_models)
            if not candidates:
                continue

            sample_jobs: list[tuple[str, str, str, int]] = []
            queued: set[tuple[str, str]] = set()

            def queue_sql(target_db: str, sql_text: str) -> None:
                normalized = normalize_sql(sql_text)
                key = (target_db, normalized)
                if not normalized or key in queued:
                    return
                db_path = resolve_db_path(db_root, target_db)
                sample_jobs.append((str(db_path), target_db, normalized, timeout))
                queued.add(key)

            queue_sql(gold_db_id, gold_sql)
            for candidate in candidates:
                queue_sql(db_id, candidate.get("sql", ""))

            results = executor.run(sample_jobs)
            sample_job_results: dict[tuple[str, str], tuple[frozenset[Tuple[object, ...]] | None, str | None]] = {}
            for result_db_id, sql_text, rows, error in results:
                sample_job_results[(result_db_id, sql_text)] = (rows, error)

            gold_rows, _ = sample_job_results.get((gold_db_id, gold_sql), (None, None))

            total += 1
            _ensure_difficulty_metrics(difficulty_metrics, difficulty, ks)
            difficulty_totals[difficulty] += 1
            if (i + 1) % 50 == 0 or i + 1 == len(samples):
                print(
                    f"Progress: {i + 1}/{len(samples)} samples processed ({(i + 1)/len(samples)*100:.1f}%)"
                )

            indices = list(range(len(candidates)))
            if shuffle_candidates and rng is not None:
                rng.shuffle(indices)
            max_considered = min(len(indices), max_k)
            considered_candidates: list[dict] = []
            pass_correct_list: list[bool] = []
            for offset in range(max_considered):
                idx = indices[offset]
                candidate = candidates[idx]
                sql_text = normalize_sql(candidate.get("sql"))
                rows, _ = sample_job_results.get((db_id, sql_text), (None, None))
                position = offset + 1
                considered_candidates.append(
                    {
                        "position": position,
                        "candidate_index": candidate.get("index", idx),
                        "sql": sql_text,
                    }
                )
                pass_correct_list.append(is_correct(rows, gold_rows))

            for k in ks:
                if any(pass_correct_list[: min(k, len(pass_correct_list))]):
                    counts[k] += 1
                    difficulty_pass_counts[difficulty][k] += 1

            if not pass_correct_list or not any(pass_correct_list):
                pass_failures.append(
                    {
                        "question_id": question_id,
                        "db_id": db_id,
                        "max_k": max_k,
                        "considered": considered_candidates,
                        "candidate_pool_size": len(candidates),
                    }
                )

        summary = run_cached_voting(
            candidates,
            sample_job_results,
            db_id,
            voting_weights=voting_weights,
            triple_vote_limit=triple_vote_limit,
        )
        vote_entry = _build_vote_score_entry(
            question_id=question_id,
            db_id=db_id,
            summary=summary,
        )
        if vote_entry:
            vote_score_entries.append(vote_entry)
        records = summary.get("records", []) if summary else []
        triple_list = [record.get("triples") or [] for record in records]
        valid_list = [bool(record.get("success")) for record in records]
        result_list: list[frozenset[Tuple[object, ...]] | None] = []
        correct_list: list[bool] = []
        error_list: list[str | None] = []
        for candidate in candidates:
            sql_text = normalize_sql(candidate.get("sql"))
            rows, error = sample_job_results.get((db_id, sql_text), (None, None))
            result_list.append(rows)
            correct_list.append(is_correct(rows, gold_rows))
            error_list.append(error)

        maj_idx, _maj_score, _freq_score, _group_id_list = get_frequency_and_group_id(
            triple_list,
            result_list,
            valid_list,
        )
        if candidates:
            major_evaluated += 1
            difficulty_major_evaluated[difficulty] += 1
            major_is_correct = correct_list[maj_idx]
            if major_is_correct:
                major_correct += 1
                difficulty_major_correct[difficulty] += 1
            pass_any = any(pass_correct_list)
            if not major_is_correct and pass_any:
                first_pass_idx = pass_correct_list.index(True)
                pass_candidate = considered_candidates[first_pass_idx]
                major_failures.append(
                    {
                        "question_id": question_id,
                        "db_id": db_id,
                        "major_sql": normalize_sql(candidates[maj_idx].get("sql")),
                        "pass_sql": pass_candidate.get("sql"),
                        "pass_position": pass_candidate.get("position"),
                        "candidate_index": pass_candidate.get("candidate_index"),
                    }
                )
            major_detail_entries.append(
                {
                    "question_id": question_id,
                    "db_id": db_id,
                    "winner_local_index": maj_idx,
                    "major_sql": normalize_sql(candidates[maj_idx].get("sql")),
                    "major_correct": major_is_correct,
                    "major_error": error_list[maj_idx],
                }
            )

    finally:
        executor.close()

    return (
        counts,
        total,
        major_evaluated,
        major_correct,
        major_failures,
        pass_failures,
        vote_score_entries,
        major_detail_entries,
        difficulty_metrics,
    )
def main() -> None:
    default_workers = max(1, min(8, os.cpu_count() or 1))
    parser = argparse.ArgumentParser(description="Compute pass@k metrics via SQL execution (optimized)")
    parser.add_argument("--input", required=True, help="Path to experiment JSON file")
    parser.add_argument(
        "--db-root", required=True, help="Directory containing <db_id>/<db_id>.sqlite databases"
    )
    parser.add_argument(
        "--ks",
        nargs="+",
        default=["8,16"],
        help="Space and/or comma separated list of k values (default: 8,16)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help=f"Number of worker processes for SQL execution (default: {default_workers})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1000,
        help="Per-SQL timeout in seconds (default: 1000, set to 0 for no timeout)",
    )
    parser.add_argument(
        "--candidate-pool-size",
        type=int,
        default=32,
        help=(
            "Number of sql_candidates per sample considered before shuffling; "
            "set to 0 to use all (default: 32 to match pass@32)"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for shuffling candidate order (default: 42)",
    )
    parser.add_argument(
        "--no-shuffle",
        action="store_true",
        help="Disable candidate shuffling (use ranked order instead of random sampling)",
    )
    parser.add_argument(
        "--exclude-auto-correct",
        action="store_true",
        help="Exclude candidates with auto_correct_attempts == 1 before sampling",
    )
    parser.add_argument(
        "--source-model",
        type=str,
        default="SQLGOVERNOR-GEN",
        help=(
            "Only keep sql_candidates whose source_model matches (case-insensitive). "
            "Use 'all' to disable filtering (default: SQLGOVERNOR-GEN)."
        ),
    )
    parser.add_argument(
        "--triple-vote-limit",
        type=int,
        default=Settings.SQL_RESULT_VOTING_TRIPLE_LIMIT,
        help=(
            "How many earliest candidates contribute triples during voting. "
            "Defaults to SQL_RESULT_VOTING_TRIPLE_LIMIT."
        ),
    )
    parser.add_argument(
        "--voting-weights",
        type=str,
        default=None,
        help=(
            "Comma-separated weights for candidate positions (higher=earlier). "
            "Overrides SQL_RESULT_VOTING_WEIGHTS if provided."
        ),
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help=(
            "Execute SQLs sample-by-sample to avoid caching the entire dataset (less memory, potentially slower)."
        ),
    )
    parser.add_argument(
        "--gold",
        type=Path,
        default=None,
        help="Path to the BIRD gold JSON (defaults to the 'input' value inside the experiment)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to save pass@k summary (txt). Defaults to <input>.pass.txt",
    )
    parser.add_argument(
        "--pass-fail-output",
        type=Path,
        default=None,
        help=(
            "Path to save pass@k failure cases (txt). Defaults to <input>.pass_failures.txt"
        ),
    )
    parser.add_argument(
        "--vote-score-output",
        type=Path,
        default=None,
        help=(
            "Optional path to save detailed voting scores as JSON. "
            "Skipped when not provided."
        ),
    )
    parser.add_argument(
        "--major-detail-output",
        type=Path,
        default=None,
        help=(
            "Optional path to save per-sample majority correctness (JSON). "
            "Skipped when not provided."
        ),
    )
    args = parser.parse_args()

    ks = parse_k_values(args.ks)
    input_path = Path(args.input)
    output_path = args.output or Path(f"{input_path}.pass.txt")
    pass_fail_output_path = args.pass_fail_output or Path(f"{input_path}.pass_failures.txt")
    vote_score_output_path = args.vote_score_output
    major_detail_output_path = args.major_detail_output
    db_root = Path(args.db_root)
    payload, results = load_experiment(input_path)
    if not results:
        raise ValueError("No results found in input JSON")

    gold_path = args.gold
    if gold_path is None:
        experiment_input = payload.get("input")
        if not experiment_input:
            raise ValueError(
                "Experiment JSON must include 'input' pointing to the gold dev file or pass --gold"
            )
        tentative = Path(experiment_input)
        if tentative.exists():
            gold_path = tentative
        else:
            gold_path = (input_path.parent / experiment_input).resolve()
    if not gold_path.exists():
        remapped_gold = _remap_legacy_path(gold_path)
        if remapped_gold != gold_path:
            print(f"Gold file {gold_path} not found; trying {remapped_gold}")
            gold_path = remapped_gold
    if not gold_path.exists():
        raise FileNotFoundError(f"Gold file not found: {gold_path}")
    gold_sql_map = load_gold_sql_map(gold_path)

    source_models = parse_source_models(args.source_model)

    if args.candidate_pool_size and args.candidate_pool_size > 0:
        max_k = max(ks)
        if args.candidate_pool_size < max_k:
            print(
                f"candidate_pool_size={args.candidate_pool_size} < max_k={max_k}; "
                f"using {max_k} so pass@{max_k} can see enough candidates."
            )
            args.candidate_pool_size = max_k

    rng = None if args.no_shuffle else random.Random(args.seed)
    cli_weights = parse_weight_values(args.voting_weights)
    config_weights = parse_weight_values(Settings.SQL_RESULT_VOTING_WEIGHTS)
    voting_weights = cli_weights or config_weights or None
    triple_vote_limit = args.triple_vote_limit

    if args.streaming:
        print(
            f"Streaming execution: processing samples sequentially with {args.workers} workers per batch..."
        )
        (
            counts,
            total,
            major_evaluated,
            major_correct,
            major_failures,
            pass_failures,
            vote_score_entries,
            major_detail_entries,
            difficulty_metrics,
        ) = evaluate_samples_streaming(
            results,
            ks,
            rng,
            args.candidate_pool_size,
            shuffle_candidates=not args.no_shuffle,
            exclude_auto_correct=args.exclude_auto_correct,
            source_models=source_models,
            gold_sql_map=gold_sql_map,
            db_root=db_root,
            workers=args.workers,
            timeout=args.timeout,
            voting_weights=voting_weights,
            triple_vote_limit=triple_vote_limit,
        )
    else:
        job_map = build_job_map(
            results,
            args.candidate_pool_size,
            db_root,
            exclude_auto_correct=args.exclude_auto_correct,
            source_models=source_models,
            gold_sql_map=gold_sql_map,
        )
        unique_dbs = {path.parent for path in job_map.values()}
        print(
            f"Executing {len(job_map)} unique SQLs across {len(unique_dbs)} DBs with {args.workers} workers..."
        )
        job_results = execute_jobs(job_map, args.workers, args.timeout)

        (
            counts,
            total,
            major_evaluated,
            major_correct,
            major_failures,
            pass_failures,
            vote_score_entries,
            major_detail_entries,
            difficulty_metrics,
        ) = evaluate_samples(
            results,
            ks,
            job_results,
            rng,
            args.candidate_pool_size,
            shuffle_candidates=not args.no_shuffle,
            exclude_auto_correct=args.exclude_auto_correct,
            source_models=source_models,
            gold_sql_map=gold_sql_map,
            db_root=db_root,
            voting_weights=voting_weights,
            triple_vote_limit=triple_vote_limit,
        )
    summary_lines: list[str] = []
    for k in ks:
        ratio = counts[k] / total if total else 0.0
        line = f"pass@{k}: {counts[k]} / {total} ({ratio:.4f})"
        summary_lines.append(line)
        print(line)
    if total == 0:
        print("No eligible samples after filtering; pass@k metrics remain zero.")

    if major_evaluated:
        major_ratio = major_correct / major_evaluated
        major_line = (
            f"major voting accuracy: {major_correct} / {major_evaluated} ({major_ratio:.4f})"
        )
        summary_lines.append(major_line)
        print(major_line)
    else:
        print("No majority-voting predictions were evaluated (missing candidate_sql).")

    difficulty_totals = difficulty_metrics.get("totals") if difficulty_metrics else {}
    if difficulty_totals:
        header = "Difficulty breakdown:"
        summary_lines.append(header)
        print(header)
        difficulty_pass_counts = difficulty_metrics.get("pass_counts", {})
        difficulty_major_evaluated = difficulty_metrics.get("major_evaluated", {})
        difficulty_major_correct = difficulty_metrics.get("major_correct", {})
        for difficulty in sort_difficulty_keys(difficulty_totals.keys()):
            total_by_diff = difficulty_totals.get(difficulty, 0)
            for k in ks:
                count_by_diff = difficulty_pass_counts.get(difficulty, {}).get(k, 0)
                ratio = count_by_diff / total_by_diff if total_by_diff else 0.0
                line = (
                    f"pass@{k} ({difficulty}): {count_by_diff} / {total_by_diff} "
                    f"({ratio:.4f})"
                )
                summary_lines.append(line)
                print(line)
            evaluated_by_diff = difficulty_major_evaluated.get(difficulty, 0)
            if evaluated_by_diff:
                major_ratio = difficulty_major_correct.get(difficulty, 0) / evaluated_by_diff
                major_line = (
                    f"major voting accuracy ({difficulty}): "
                    f"{difficulty_major_correct.get(difficulty, 0)} / {evaluated_by_diff} "
                    f"({major_ratio:.4f})"
                )
            else:
                major_line = f"major voting accuracy ({difficulty}): 0 / 0 (0.0000)"
            summary_lines.append(major_line)
            print(major_line)

    failure_lines: list[str] = []
    if major_failures:
        header = (
            f"Major voting failed but pass@k succeeded on {len(major_failures)} samples:\\n"
        )
        print(header)
        failure_lines.append(header.rstrip("\n"))
        for failure in major_failures:
            block = ["=" * 80]
            block.append(
                f"question_id={failure['question_id']} db_id={failure['db_id']} "
                f"pass@k_position={failure['pass_position']} candidate_index={failure['candidate_index']}"
            )
            block.append("major_voting_sql:")
            block.append(failure["major_sql"] or "<empty>")
            block.append("pass@k_sql:")
            block.append(failure["pass_sql"])
            text_block = "\n".join(block)
            print(text_block + "\n")
            failure_lines.append(text_block)
    else:
        message = "No samples where major voting was wrong but pass@k found a correct SQL."
        print(message)
        failure_lines.append(message)

    pass_fail_lines: list[str] = []
    max_k_value = max(ks)
    if pass_failures:
        header = f"pass@{max_k_value} failed on {len(pass_failures)} samples:\n"
        print(header)
        pass_fail_lines.append(header.rstrip("\n"))
        for failure in pass_failures:
            considered = failure.get("considered") or []
            block = ["=" * 80]
            block.append(
                f"question_id={failure['question_id']} db_id={failure['db_id']} "
                f"considered={len(considered)} candidate_pool_size={failure['candidate_pool_size']} "
                f"max_k={failure['max_k']}"
            )
            block.append("considered_sqls:")
            if considered:
                for candidate in considered:
                    sql_text = candidate.get("sql") or "<empty>"
                    block.append(
                        f"[pos={candidate.get('position')} idx={candidate.get('candidate_index')}] "
                        f"{sql_text}"
                    )
            else:
                block.append("<no successful executions within evaluated candidates>")
            text_block = "\n".join(block)
            pass_fail_lines.append(text_block)
    else:
        message = f"All evaluated samples succeeded within pass@{max_k_value}."
        print(message)
        pass_fail_lines.append(message)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = "\n".join(summary_lines)
        if failure_lines:
            payload += "\n\n" + "\n".join(failure_lines)
        output_path.write_text(payload + "\n", encoding="utf-8")
        print(f"Saved pass@k summary to {output_path}")

    if pass_fail_output_path:
        pass_fail_output_path.parent.mkdir(parents=True, exist_ok=True)
        pass_payload = "\n".join(pass_fail_lines)
        pass_fail_output_path.write_text(pass_payload + "\n", encoding="utf-8")
        print(f"Saved pass@k failures to {pass_fail_output_path}")

    if vote_score_output_path:
        vote_score_output_path.parent.mkdir(parents=True, exist_ok=True)
        vote_score_output_path.write_text(
            json.dumps(vote_score_entries, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Saved vote scores to {vote_score_output_path}")

    if major_detail_output_path:
        major_detail_output_path.parent.mkdir(parents=True, exist_ok=True)
        major_detail_output_path.write_text(
            json.dumps(major_detail_entries, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Saved major correctness details to {major_detail_output_path}")


if __name__ == "__main__":
    main()
