"""Evaluate execution accuracy between predicted SQL map and gold SQLs."""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sqlite3
import time
from pathlib import Path
from typing import Iterable, Sequence, Tuple


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


def resolve_db_path(db_root: Path, db_id: str) -> Path:
    db_path = db_root / db_id / f"{db_id}.sqlite"
    if not db_path.exists():
        raise FileNotFoundError(f"Missing SQLite file for {db_id}: {db_path}")
    return db_path


def _connect_sqlite_readonly(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


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
    finally:
        conn.set_progress_handler(None, 0)
        conn.close()


def _execute_sql_job(
    job: tuple[str, str, str, int]
) -> tuple[str, str, frozenset[Tuple[object, ...]] | None, str | None]:
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
        sql = normalize_sql(item.get("SQL"))
        difficulty = item.get("difficulty")
        if question_id is None or not db_id or not sql:
            continue
        mapping[int(question_id)] = {
            "db_id": db_id,
            "sql": sql,
            "difficulty": difficulty,
        }
    if not mapping:
        raise ValueError(f"Gold file {path} did not yield any usable SQLs")
    return mapping


def load_pred_sql_map(path: Path) -> dict[int, str]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("Pred file must be a JSON object mapping question_id to SQL")
    mapping: dict[int, str] = {}
    for key, value in payload.items():
        try:
            qid = int(key)
        except (TypeError, ValueError):
            continue
        sql = normalize_sql(value if isinstance(value, str) else None)
        if sql:
            mapping[qid] = sql
    return mapping


def build_job_map(
    pred_map: dict[int, str],
    gold_map: dict[int, dict[str, str | None]],
    db_root: Path,
) -> dict[tuple[str, str], Path]:
    """Build execution jobs over the complete gold set.

    Gold queries are always executed. Predictions are added only when present; missing or empty
    predictions remain in the evaluation denominator and are counted as incorrect.
    """
    job_map: dict[tuple[str, str], Path] = {}
    for question_id, gold_entry in gold_map.items():
        db_id = gold_entry.get("db_id")
        gold_sql = normalize_sql(gold_entry.get("sql"))
        if not db_id or not gold_sql:
            continue
        db_path = resolve_db_path(db_root, db_id)
        job_map.setdefault((db_id, gold_sql), db_path)
        pred_sql = pred_map.get(question_id)
        if pred_sql:
            job_map.setdefault((db_id, pred_sql), db_path)
    return job_map


def main() -> None:
    default_workers = max(1, min(8, mp.cpu_count() or 1))
    parser = argparse.ArgumentParser(
        description="Compute execution accuracy for predicted SQL map vs gold SQL."
    )
    parser.add_argument("--pred", required=True, help="Path to prediction JSON map")
    parser.add_argument("--gold", required=True, help="Path to gold dev JSON file")
    parser.add_argument(
        "--db-root", required=True, help="Directory containing <db_id>/<db_id>.sqlite databases"
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
        "--output",
        type=Path,
        default=None,
        help="Path to save summary (txt). Defaults to <pred>.exec_match.txt",
    )
    args = parser.parse_args()

    pred_path = Path(args.pred)
    gold_path = Path(args.gold)
    db_root = Path(args.db_root)
    output_path = args.output or Path(f"{pred_path}.exec_match.txt")

    pred_map = load_pred_sql_map(pred_path)
    gold_map = load_gold_sql_map(gold_path)
    job_map = build_job_map(pred_map, gold_map, db_root)

    if not job_map:
        raise ValueError("No SQL jobs to execute after filtering.")

    print(f"Executing {len(job_map)} unique SQLs with {args.workers} workers...")
    job_results = execute_jobs(job_map, args.workers, args.timeout)

    total = len(gold_map)
    correct = 0
    missing_prediction = 0
    pred_failed = 0
    gold_execution_failed = 0
    extra_predictions = len(set(pred_map) - set(gold_map))
    difficulty_totals: dict[str, int] = {}
    difficulty_correct: dict[str, int] = {}

    for idx, (question_id, gold_entry) in enumerate(gold_map.items()):
        db_id = gold_entry.get("db_id")
        gold_sql = normalize_sql(gold_entry.get("sql"))
        difficulty = normalize_difficulty(gold_entry.get("difficulty"))
        difficulty_totals[difficulty] = difficulty_totals.get(difficulty, 0) + 1

        pred_sql = pred_map.get(question_id)
        if not pred_sql:
            missing_prediction += 1
        elif not db_id or not gold_sql:
            gold_execution_failed += 1
        else:
            gold_rows, gold_error = job_results.get((db_id, gold_sql), (None, None))
            if gold_rows is None:
                gold_execution_failed += 1
            else:
                pred_rows, pred_error = job_results.get((db_id, pred_sql), (None, None))
                if pred_rows is None:
                    pred_failed += 1
                elif pred_rows == gold_rows:
                    correct += 1
                    difficulty_correct[difficulty] = difficulty_correct.get(difficulty, 0) + 1

        if (idx + 1) % 50 == 0 or (idx + 1) == total:
            print(
                f"Progress: {idx + 1}/{total} gold questions processed "
                f"({(idx + 1)/total*100:.1f}%)"
            )

    accuracy = correct / total if total else 0.0
    summary_lines = [
        f"execution accuracy: {correct} / {total} ({accuracy:.4f})",
        f"gold questions: {len(gold_map)}",
        f"valid predictions: {len(pred_map)}",
        f"missing predictions: {missing_prediction}",
        f"prediction execution failures: {pred_failed}",
        f"gold execution failures: {gold_execution_failed}",
        f"extra predictions ignored: {extra_predictions}",
    ]
    if difficulty_totals:
        summary_lines.append("difficulty breakdown:")
        for difficulty in sort_difficulty_keys(difficulty_totals.keys()):
            total_by_diff = difficulty_totals.get(difficulty, 0)
            correct_by_diff = difficulty_correct.get(difficulty, 0)
            acc_by_diff = correct_by_diff / total_by_diff if total_by_diff else 0.0
            summary_lines.append(
                f"accuracy ({difficulty}): {correct_by_diff} / {total_by_diff} "
                f"({acc_by_diff:.4f})"
            )
    summary_text = "\n".join(summary_lines) + "\n"
    print(summary_text.strip())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(summary_text, encoding="utf-8")
    print(f"Saved summary to {output_path}")


if __name__ == "__main__":
    main()
