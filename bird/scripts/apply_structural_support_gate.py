#!/usr/bin/env python3
"""Build selector-style output using a deterministic structural-support gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.tune_selection_score_weights import load_checkpoint_rows  # noqa: E402
from src.selection.structural_support_gate import build_structural_support_decision  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply deterministic structural-support gate on low-confidence merged ties."
    )
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--qid-list", type=Path, default=None)
    return parser.parse_args()


def load_qid_filter(path: Path | None) -> set[int] | None:
    if path is None:
        return None
    if not path.exists():
        return set()
    return {
        int(line.strip())
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def main() -> None:
    args = parse_args()
    run_payload = json.loads(args.run.read_text(encoding="utf-8"))
    run_map = {int(row["question_id"]): row for row in run_payload.get("results") or []}
    checkpoint_rows = load_checkpoint_rows(args.checkpoint)
    qid_filter = load_qid_filter(args.qid_list)

    results: list[dict[str, Any]] = []
    gate_passed_count = 0
    skipped_missing_run = 0
    skipped_by_qid_filter = 0

    for row in checkpoint_rows:
        qid = int(row["question_id"])
        if qid_filter is not None and qid not in qid_filter:
            skipped_by_qid_filter += 1
            continue
        sample = run_map.get(qid)
        if sample is None:
            skipped_missing_run += 1
            continue
        decision = build_structural_support_decision(
            checkpoint_row=row,
            sample=sample,
        )
        gate_passed_count += int(bool(decision.get("gate_passed")))
        results.append(decision)

    payload = {
        "summary": {
            "selector_type": "structural_support_gate",
            "evaluated": len(results),
            "gate_passed_count": gate_passed_count,
            "gate_passed_ratio": gate_passed_count / len(results) if results else 0.0,
            "qid_filter_size": len(qid_filter) if qid_filter is not None else None,
            "skipped_missing_run": skipped_missing_run,
            "skipped_by_qid_filter": skipped_by_qid_filter,
        },
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(f"output={args.output}")


if __name__ == "__main__":
    main()
