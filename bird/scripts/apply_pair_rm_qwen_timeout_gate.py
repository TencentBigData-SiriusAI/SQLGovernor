#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.tune_selection_score_weights import load_checkpoint_rows, normalize_difficulty  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply unified selection: pair_rm baseline + qwen gate + timeout specialist patch."
    )
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--selector-output", type=Path, required=True)
    parser.add_argument("--timeout-router-output", type=Path, required=True)
    parser.add_argument("--timeout-probe-output", type=Path, required=True)
    parser.add_argument(
        "--timeout-risk-source-run",
        type=Path,
        default=None,
        help="Optional run JSON used to recover timeout-risk samples when phase5 no longer contains timeout candidates.",
    )
    parser.add_argument(
        "--timeout-risk-qids",
        type=Path,
        default=None,
        help="Optional newline-delimited qid list to mark timeout-risk samples.",
    )
    parser.add_argument("--output-jsonl", type=Path, required=True)
    parser.add_argument("--output-final-json", type=Path, required=True)
    parser.add_argument("--output-final-map-json", type=Path, required=True)
    parser.add_argument(
        "--general-difficulties",
        type=str,
        default="all",
        help="Comma-separated difficulties for general qwen gate. Use 'all' to allow every difficulty.",
    )
    parser.add_argument("--max-merged-gap", type=float, default=0.0)
    parser.add_argument("--max-maj-gap", type=float, default=0.0)
    parser.add_argument("--max-rm-gap", type=float, default=0.0)
    parser.add_argument("--max-cluster-count", type=int, default=6)
    parser.add_argument("--max-top-size", type=int, default=8)
    parser.add_argument(
        "--timeout-max-cluster-count",
        type=int,
        default=0,
        help="Apply timeout specialist only when general selector cluster_count <= this threshold.",
    )
    return parser.parse_args()


def normalize_sql(sql: str | None) -> str:
    return " ".join((sql or "").split()).strip()


def compute_gap(values: list[float] | None) -> float:
    arr = [float(x) for x in (values or [])]
    if not arr:
        return 0.0
    arr = sorted(arr, reverse=True)
    best = arr[0]
    second = arr[1] if len(arr) > 1 else best
    return best - second


def parse_allowed_difficulties(raw: str) -> set[str]:
    tokens = {token.strip().lower() for token in raw.split(",") if token.strip()}
    if not tokens or "all" in tokens:
        return {"simple", "moderate", "challenging", "unknown"}
    return tokens


def build_timeout_risk_qids(run_payload: dict[str, Any]) -> set[int]:
    timeout_qids: set[int] = set()
    for sample in run_payload.get("results") or []:
        qid = int(sample["question_id"])
        for cand in sample.get("sql_candidates") or []:
            execution = cand.get("execution") or {}
            if execution.get("status") == "timeout":
                timeout_qids.add(qid)
                break
    return timeout_qids


def load_timeout_risk_qids(
    *,
    run_payload: dict[str, Any],
    timeout_risk_source_payload: dict[str, Any] | None,
    timeout_risk_qids_path: Path | None,
) -> set[int]:
    timeout_qids = build_timeout_risk_qids(run_payload)
    if timeout_risk_source_payload is not None:
        timeout_qids |= build_timeout_risk_qids(timeout_risk_source_payload)
    if timeout_risk_qids_path is not None and timeout_risk_qids_path.exists():
        timeout_qids |= {
            int(line.strip())
            for line in timeout_risk_qids_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    return timeout_qids


def load_selector_map(path: Path) -> dict[int, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {int(row["question_id"]): row for row in payload.get("results") or []}


def selector_cluster_count(selector_row: dict[str, Any] | None) -> int:
    if selector_row is None:
        return 0
    if selector_row.get("selector_type") == "structural_support_gate":
        return int(selector_row.get("top_group_count") or 0)
    return len(selector_row.get("clusters") or [])


def gate_enabled(
    *,
    difficulty: str,
    allowed_difficulties: set[str],
    checkpoint_row: dict[str, Any],
    selector_row: dict[str, Any] | None,
    max_merged_gap: float,
    max_maj_gap: float,
    max_rm_gap: float,
    max_cluster_count: int,
    max_top_size: int,
) -> bool:
    if difficulty not in allowed_difficulties:
        return False
    if selector_row is None:
        return False
    chosen_sql = (selector_row.get("chosen_sql") or "").strip()
    if compute_gap(checkpoint_row.get("merged_score")) > max_merged_gap:
        return False
    if compute_gap(checkpoint_row.get("maj_score_refine")) > max_maj_gap:
        return False
    if compute_gap(checkpoint_row.get("rm_score")) > max_rm_gap:
        return False
    if selector_row.get("selector_type") == "structural_support_gate":
        return bool(selector_row.get("gate_passed")) and bool(chosen_sql)
    if not chosen_sql:
        return False
    clusters = selector_row.get("clusters") or []
    top_size = max([int(cluster.get("size", 0)) for cluster in clusters], default=0)
    if len(clusters) > max_cluster_count:
        return False
    if top_size > max_top_size:
        return False
    return True


def main() -> None:
    args = parse_args()
    run_payload = json.loads(args.run.read_text(encoding="utf-8"))
    timeout_risk_source_payload = (
        json.loads(args.timeout_risk_source_run.read_text(encoding="utf-8"))
        if args.timeout_risk_source_run
        else None
    )
    run_map = {int(row["question_id"]): row for row in run_payload.get("results") or []}
    difficulty_map = {
        int(row["question_id"]): normalize_difficulty(row.get("difficulty"))
        for row in run_payload.get("results") or []
    }
    timeout_risk_qids = load_timeout_risk_qids(
        run_payload=run_payload,
        timeout_risk_source_payload=timeout_risk_source_payload,
        timeout_risk_qids_path=args.timeout_risk_qids,
    )

    checkpoint_rows = load_checkpoint_rows(args.checkpoint)
    selector_map = load_selector_map(args.selector_output)
    timeout_router_map = load_selector_map(args.timeout_router_output)
    timeout_probe_map = load_selector_map(args.timeout_probe_output)

    allowed_difficulties = parse_allowed_difficulties(args.general_difficulties)

    output_rows: list[dict[str, Any]] = []
    final_rows: list[dict[str, Any]] = []
    final_map: dict[str, str] = {}

    general_gate_enabled_count = 0
    timeout_specialist_enabled_count = 0

    for row in checkpoint_rows:
        qid = int(row["question_id"])
        difficulty = difficulty_map.get(qid, "unknown")
        pred_sqls = row.get("pred_sqls") or []
        merged_idx = int(row.get("merged_idx", 0))
        baseline_sql = pred_sqls[merged_idx] if 0 <= merged_idx < len(pred_sqls) else ""

        selector_row = selector_map.get(qid)
        use_general_gate = gate_enabled(
            difficulty=difficulty,
            allowed_difficulties=allowed_difficulties,
            checkpoint_row=row,
            selector_row=selector_row,
            max_merged_gap=args.max_merged_gap,
            max_maj_gap=args.max_maj_gap,
            max_rm_gap=args.max_rm_gap,
            max_cluster_count=args.max_cluster_count,
            max_top_size=args.max_top_size,
        )
        general_sql = baseline_sql
        if use_general_gate:
            general_sql = (selector_row.get("chosen_sql") or "").strip()
            general_gate_enabled_count += 1

        final_sql = general_sql
        timeout_override = False
        timeout_router_sql = ""
        timeout_probe_sql = ""
        timeout_cluster_count = selector_cluster_count(selector_row)

        if qid in timeout_risk_qids:
            timeout_router_sql = (timeout_router_map.get(qid, {}).get("chosen_sql") or "").strip()
            timeout_probe_sql = (timeout_probe_map.get(qid, {}).get("chosen_sql") or "").strip()
            timeout_specialist_agreement = (
                bool(timeout_router_sql)
                and normalize_sql(timeout_router_sql) == normalize_sql(timeout_probe_sql)
            )
            if timeout_specialist_agreement and timeout_cluster_count <= args.timeout_max_cluster_count:
                final_sql = timeout_router_sql
                timeout_override = True
                timeout_specialist_enabled_count += 1
        else:
            timeout_specialist_agreement = False

        record = {
            "question_id": qid,
            "db_id": run_map.get(qid, {}).get("db_id"),
            "difficulty": difficulty,
            "timeout_risk": qid in timeout_risk_qids,
            "baseline_pair_rm_sql": baseline_sql,
            "general_gate_mode": (selector_row or {}).get("selector_type", "qwen_selector"),
            "general_qwen_sql": (selector_row.get("chosen_sql") if selector_row else None),
            "general_gate_reason": (selector_row or {}).get("reason"),
            "general_gate_enabled": use_general_gate,
            "general_selector_cluster_count": timeout_cluster_count,
            "timeout_router_sql": timeout_router_sql or None,
            "timeout_probe_sql": timeout_probe_sql or None,
            "timeout_specialist_agreement": timeout_specialist_agreement,
            "timeout_specialist_enabled": timeout_override,
            "final_sql": final_sql,
        }
        output_rows.append(record)
        final_rows.append({"question_id": qid, "final_sql": final_sql})
        final_map[str(qid)] = final_sql

    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with args.output_jsonl.open("w", encoding="utf-8") as handle:
        for row in output_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    args.output_final_json.write_text(
        json.dumps(final_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    args.output_final_map_json.write_text(
        json.dumps(final_map, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = {
        "evaluated": len(output_rows),
        "general_gate_enabled_count": general_gate_enabled_count,
        "timeout_risk_count": len(timeout_risk_qids),
        "timeout_specialist_enabled_count": timeout_specialist_enabled_count,
        "general_difficulties": sorted(allowed_difficulties),
        "max_merged_gap": args.max_merged_gap,
        "max_maj_gap": args.max_maj_gap,
        "max_rm_gap": args.max_rm_gap,
        "max_cluster_count": args.max_cluster_count,
        "max_top_size": args.max_top_size,
        "timeout_max_cluster_count": args.timeout_max_cluster_count,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"output_jsonl={args.output_jsonl}")
    print(f"output_final_json={args.output_final_json}")
    print(f"output_final_map_json={args.output_final_map_json}")


if __name__ == "__main__":
    main()
