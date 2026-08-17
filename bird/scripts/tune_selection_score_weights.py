#!/usr/bin/env python3
"""Tune selection-score weights on a validation split using execution correctness."""

from __future__ import annotations

import argparse
import itertools
import json
import multiprocessing as mp
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_INPUT = Path("experiments/experiments/dev_run_0227_v1_selection.checkpoint.jsonl")
DEFAULT_OUTPUT = PROJECT_ROOT / "experiments" / "selection_weight_tuning_dev0227_val10.json"


def parse_float_grid(values: list[str]) -> list[float]:
    output: list[float] = []
    for value in values:
        for chunk in value.split(","):
            token = chunk.strip()
            if token:
                output.append(float(token))
    return sorted(set(output))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tune selection score weights on a val split.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--gold", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-mod", type=int, default=10)
    parser.add_argument("--sample-rem", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--screen-score-run",
        type=Path,
        default=None,
        help="Optional scored run JSON path that provides screen_score per (question_id, sql).",
    )
    parser.add_argument("--w-refine", nargs="+", default=["0,0.5,1,2"])
    parser.add_argument("--w-maj", nargs="+", default=["0,0.5,1,2"])
    parser.add_argument("--w-freq", nargs="+", default=["0,0.5,1"])
    parser.add_argument("--w-rm", nargs="+", default=["0,0.5,1,2,4"])
    parser.add_argument("--w-screen", nargs="+", default=["0,0.5,1,2,4"])
    parser.add_argument("--w-valid", nargs="+", default=["0,0.5,1"])
    parser.add_argument("--top-k", type=int, default=30)
    return parser


def load_checkpoint_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def normalize_sql(sql: str | None) -> str:
    return (sql or "").strip()


def normalize_difficulty(value: str | None) -> str:
    if value is None:
        return "unknown"
    normalized = str(value).strip().lower()
    return normalized or "unknown"


def _ensure_difficulty_metrics(
    metrics: dict[str, dict],
    difficulty: str,
) -> None:
    totals: dict[str, int] = metrics["totals"]
    major_evaluated: dict[str, int] = metrics["major_evaluated"]
    major_correct: dict[str, int] = metrics["major_correct"]
    if difficulty not in totals:
        totals[difficulty] = 0
        major_evaluated[difficulty] = 0
        major_correct[difficulty] = 0


def load_gold_sql_map(path: Path) -> dict[int, dict[str, str | None]]:
    entries = json.loads(path.read_text(encoding="utf-8"))
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
    return mapping


def resolve_db_path(db_root: Path, db_id: str) -> Path:
    db_path = db_root / db_id / f"{db_id}.sqlite"
    if not db_path.exists():
        raise FileNotFoundError(f"Missing SQLite file for {db_id}: {db_path}")
    return db_path


def canonicalize_rows(rows: list[tuple[Any, ...]] | None) -> frozenset[tuple[object, ...]]:
    canonical_rows: list[tuple[object, ...]] = []
    if rows is None:
        return frozenset()
    for row in rows:
        canonical_rows.append(tuple(row))
    return frozenset(canonical_rows)


def _run_single_sql(
    db_path: str,
    sql: str,
    timeout: int,
) -> frozenset[tuple[object, ...]]:
    conn = sqlite3.connect(db_path, timeout=5.0)
    start_time = time.time()

    def _progress_handler() -> None:
        if timeout > 0 and time.time() - start_time > timeout:
            raise TimeoutError("SQL execution timed out")

    try:
        if timeout > 0:
            conn.set_progress_handler(_progress_handler, 1000)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return canonicalize_rows(rows)
    finally:
        conn.set_progress_handler(None, 0)
        conn.close()


def _execute_sql_job(
    job: tuple[str, str, str, int]
) -> tuple[str, str, frozenset[tuple[object, ...]] | None, str | None]:
    db_path, db_id, sql, timeout = job
    try:
        rows = _run_single_sql(db_path, sql, timeout)
        return db_id, sql, rows, None
    except TimeoutError:
        return db_id, sql, None, "Timeout"
    except Exception as exc:  # pylint: disable=broad-except
        return db_id, sql, None, str(exc)


def execute_jobs(
    job_map: dict[tuple[str, str], Path],
    workers: int,
    timeout: int,
) -> dict[tuple[str, str], tuple[frozenset[tuple[object, ...]] | None, str | None]]:
    jobs = [(str(path), db_id, sql, timeout) for (db_id, sql), path in job_map.items()]
    results: dict[tuple[str, str], tuple[frozenset[tuple[object, ...]] | None, str | None]] = {}
    if workers <= 1:
        for job in jobs:
            db_id, sql, rows, error = _execute_sql_job(job)
            results[(db_id, sql)] = (rows, error)
        return results
    with mp.Pool(processes=workers) as pool:
        for db_id, sql, rows, error in pool.imap_unordered(_execute_sql_job, jobs, chunksize=max(1, len(jobs) // (workers * 2))):
            results[(db_id, sql)] = (rows, error)
    return results


def is_correct(
    pred: frozenset[tuple[object, ...]] | None,
    gt: frozenset[tuple[object, ...]] | None,
) -> bool:
    if pred is None and gt is None:
        return True
    if pred is None or gt is None:
        return False
    return pred == gt


def sample_rows(rows: list[dict[str, Any]], sample_mod: int, sample_rem: int) -> list[dict[str, Any]]:
    return [row for row in rows if int(row.get("question_id")) % sample_mod == sample_rem]


def build_job_map_for_checkpoint(
    rows: list[dict[str, Any]],
    gold_sql_map: dict[int, dict[str, str | None]],
    db_root: Path,
) -> dict[tuple[str, str], Path]:
    job_map: dict[tuple[str, str], Path] = {}
    for row in rows:
        question_id = row.get("question_id")
        if question_id is None:
            continue
        gold_entry = gold_sql_map.get(int(question_id))
        if not gold_entry:
            continue
        db_id = gold_entry.get("db_id")
        gold_sql = normalize_sql(gold_entry.get("sql"))
        if not db_id or not gold_sql:
            continue
        job_map.setdefault((db_id, gold_sql), resolve_db_path(db_root, db_id))
        for sql in row.get("pred_sqls", []):
            sql_text = normalize_sql(sql)
            if sql_text:
                job_map.setdefault((db_id, sql_text), resolve_db_path(db_root, db_id))
    return job_map


def load_screen_score_map(path: Path) -> dict[tuple[int, str], float]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError(f"Scored run JSON missing results list: {path}")
    score_map: dict[tuple[int, str], float] = {}
    for sample in results:
        question_id = sample.get("question_id")
        if question_id is None:
            continue
        for candidate in sample.get("sql_candidates", []):
            if "screen_score" not in candidate:
                continue
            sql = normalize_sql(candidate.get("sql"))
            if not sql:
                continue
            key = (int(question_id), sql)
            score = float(candidate["screen_score"])
            if key not in score_map or score > score_map[key]:
                score_map[key] = score
    return score_map


def attach_screen_scores(
    rows: list[dict[str, Any]],
    score_map: dict[tuple[int, str], float] | None,
) -> list[dict[str, Any]]:
    if not score_map:
        return rows
    attached: list[dict[str, Any]] = []
    for row in rows:
        qid = row.get("question_id")
        record = dict(row)
        screen_scores = []
        for sql in row.get("pred_sqls", []):
            key = (int(qid), normalize_sql(sql))
            screen_scores.append(float(score_map.get(key, 0.0)))
        record["screen_score"] = screen_scores
        attached.append(record)
    return attached


def minmax(values: list[float]) -> list[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if hi <= lo:
        return [0.0 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]


def score_row(
    row: dict[str, Any],
    weights: tuple[float, float, float, float, float, float],
) -> tuple[int, list[float]]:
    w_refine, w_maj, w_freq, w_rm, w_screen, w_valid = weights
    refine = minmax([float(x) for x in (row.get("maj_score_refine") or [])])
    maj = minmax([float(x) for x in (row.get("maj_score") or [])])
    freq = minmax([float(x) for x in (row.get("freq_score") or [])])
    rm = minmax([float(x) for x in (row.get("rm_score") or [])])
    screen = minmax([float(x) for x in (row.get("screen_score") or [])])
    valid = [1.0 if bool(x) else 0.0 for x in (row.get("valid_list") or [])]
    n = len(row.get("pred_sqls") or [])
    scores = []
    for i in range(n):
        score = (
            w_refine * (refine[i] if i < len(refine) else 0.0)
            + w_maj * (maj[i] if i < len(maj) else 0.0)
            + w_freq * (freq[i] if i < len(freq) else 0.0)
            + w_rm * (rm[i] if i < len(rm) else 0.0)
            + w_screen * (screen[i] if i < len(screen) else 0.0)
            + w_valid * (valid[i] if i < len(valid) else 0.0)
        )
        scores.append(score)
    best_idx = max(range(n), key=scores.__getitem__) if scores else 0
    return best_idx, scores


def evaluate_config(
    rows: list[dict[str, Any]],
    gold_sql_map: dict[int, dict[str, str | None]],
    job_results: dict[tuple[str, str], tuple[frozenset[tuple[object, ...]] | None, str | None]],
    weights: tuple[float, float, float, float, float, float],
) -> dict[str, Any]:
    total = 0
    correct = 0
    difficulty_metrics: dict[str, dict] = {
        "totals": {},
        "pass_counts": {},
        "major_evaluated": {},
        "major_correct": {},
    }
    for row in rows:
        question_id = row.get("question_id")
        if question_id is None:
            continue
        gold_entry = gold_sql_map.get(int(question_id))
        if not gold_entry:
            continue
        db_id = gold_entry.get("db_id")
        gold_sql = normalize_sql(gold_entry.get("sql"))
        if not db_id or not gold_sql:
            continue

        difficulty = normalize_difficulty(gold_entry.get("difficulty"))
        _ensure_difficulty_metrics(difficulty_metrics, difficulty)
        difficulty_metrics["totals"][difficulty] += 1

        best_idx, scores = score_row(row, weights)
        pred_sqls = row.get("pred_sqls") or []
        if best_idx >= len(pred_sqls):
            continue
        pred_sql = normalize_sql(pred_sqls[best_idx])
        pred_rows, _ = job_results.get((db_id, pred_sql), (None, None))
        gold_rows, _ = job_results.get((db_id, gold_sql), (None, None))
        ok = is_correct(pred_rows, gold_rows)
        total += 1
        difficulty_metrics["major_evaluated"][difficulty] += 1
        if ok:
            correct += 1
            difficulty_metrics["major_correct"][difficulty] += 1

    difficulty_breakdown = {}
    for difficulty, total_count in difficulty_metrics["totals"].items():
        evaluated = difficulty_metrics["major_evaluated"][difficulty]
        correct_count = difficulty_metrics["major_correct"][difficulty]
        difficulty_breakdown[difficulty] = {
            "evaluated": evaluated,
            "correct": correct_count,
            "accuracy": correct_count / evaluated if evaluated else 0.0,
            "total": total_count,
        }

    return {
        "weights": {
            "w_refine": weights[0],
            "w_maj": weights[1],
            "w_freq": weights[2],
            "w_rm": weights[3],
            "w_screen": weights[4],
            "w_valid": weights[5],
        },
        "evaluated": total,
        "correct": correct,
        "accuracy": correct / total if total else 0.0,
        "difficulty_breakdown": difficulty_breakdown,
    }


def main() -> None:
    args = build_arg_parser().parse_args()
    rows = sample_rows(load_checkpoint_rows(args.input), args.sample_mod, args.sample_rem)
    score_map = load_screen_score_map(args.screen_score_run) if args.screen_score_run else None
    rows = attach_screen_scores(rows, score_map)
    gold_sql_map = load_gold_sql_map(args.gold)
    job_map = build_job_map_for_checkpoint(rows, gold_sql_map, args.db_root)
    job_results = execute_jobs(
        job_map,
        workers=args.workers,
        timeout=args.timeout,
    )

    w_refine = parse_float_grid(args.w_refine)
    w_maj = parse_float_grid(args.w_maj)
    w_freq = parse_float_grid(args.w_freq)
    w_rm = parse_float_grid(args.w_rm)
    w_screen = parse_float_grid(args.w_screen)
    w_valid = parse_float_grid(args.w_valid)

    results = []
    for weights in itertools.product(w_refine, w_maj, w_freq, w_rm, w_screen, w_valid):
        if all(weight == 0.0 for weight in weights):
            continue
        results.append(evaluate_config(rows, gold_sql_map, job_results, weights))

    results.sort(
        key=lambda item: (
            -item["accuracy"],
            -item["difficulty_breakdown"].get("challenging", {}).get("accuracy", 0.0),
            -item["difficulty_breakdown"].get("moderate", {}).get("accuracy", 0.0),
        )
    )

    output = {
        "input": str(args.input),
        "gold": str(args.gold),
        "db_root": str(args.db_root),
        "screen_score_run": str(args.screen_score_run) if args.screen_score_run else None,
        "sample_mod": args.sample_mod,
        "sample_rem": args.sample_rem,
        "sample_size": len(rows),
        "search_space_size": len(results),
        "top_results": results[: args.top_k],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    for row in results[: args.top_k]:
        print(json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    main()
