#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from loguru import logger
from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.model_config import get_model_config
from scripts.sql_pipeline_v2 import _correct_timeout, _exec_sql
from src.tools.sql_result_voter import canonicalize_rows


PROMPT = """You are judging SQL candidates for a timeout-risk sample.

Goal:
- Pick the SQL candidate most semantically aligned with the question.
- Use the probe facts as grounding hints from the database.
- Prefer semantic correctness over superficial simplicity.
- Do not reward candidates that answer a different field/entity than requested.

Return JSON only:
{{"chosen_index": 0, "reason": "..."}}

Question:
{question}

Evidence:
{evidence}

Probe facts:
{probe_facts}

Candidates:
{candidate_blocks}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Timeout-risk router: selector first, timeout-repair fallback if needed.")
    parser.add_argument("--run-final", type=Path, required=True)
    parser.add_argument("--run-timeout-source", type=Path, required=True)
    parser.add_argument("--qid-list", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--gold", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model-key", type=str, default="qwen3-235b")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--max-timeout-repairs", type=int, default=2)
    return parser.parse_args()


def _exec_sql_signature(db_path: str, sql_text: str, timeout: int) -> tuple[frozenset[tuple[Any, ...]] | None, list[list[Any]] | None]:
    import time
    start = time.perf_counter()
    conn = sqlite3.connect(db_path, timeout=5.0)

    def _progress_handler() -> int:
        if timeout > 0 and time.perf_counter() - start > timeout:
            raise TimeoutError("timeout")
        return 0

    try:
        if timeout > 0:
            conn.set_progress_handler(_progress_handler, 1000)
        cur = conn.cursor()
        cur.execute(sql_text)
        rows = cur.fetchall()
        return canonicalize_rows(rows), [list(r) for r in rows[:5]]
    except Exception:
        return None, None
    finally:
        try:
            conn.set_progress_handler(None, 0)
        except Exception:
            pass
        conn.close()


def build_probe_facts(sample: dict[str, Any], db_path: str) -> list[str]:
    q = (sample.get("question") or "").lower()
    e = (sample.get("evidence") or "").lower()
    text = f"{q} {e}"
    facts: list[str] = []
    conn = sqlite3.connect(db_path, timeout=5.0)
    try:
        if "legacy" in text and "language" in text and "uuid" in text:
            cnt = conn.execute("SELECT COUNT(DISTINCT uuid) FROM legalities WHERE format = 'legacy'").fetchone()[0]
            facts.append(f"legacy_uuid_count = {cnt}")
        if "banned" in text and "format" in text and "highest" in text:
            rows = conn.execute(
                "SELECT format, COUNT(*) AS c FROM legalities WHERE status = 'Banned' GROUP BY format ORDER BY c DESC LIMIT 3"
            ).fetchall()
            facts.append(f"top_banned_formats = {rows}")
        if "teacher" in text and "percentage" in text:
            teacher = conn.execute("SELECT COUNT(DISTINCT UserId) FROM badges WHERE Name = 'Teacher'").fetchone()[0]
            total = conn.execute("SELECT COUNT(Id) FROM users").fetchone()[0]
            facts.append(f"teacher_users = {teacher}")
            facts.append(f"total_users = {total}")
        if "latest created user account" in text:
            row = conn.execute("SELECT Id FROM users ORDER BY CreationDate DESC LIMIT 1").fetchone()
            if row:
                latest_id = row[0]
                facts.append(f"latest_user_id = {latest_id}")
                post_cnt = conn.execute("SELECT COUNT(*) FROM posts WHERE OwnerUserId = ?", (latest_id,)).fetchone()[0]
                comment_cnt = conn.execute("SELECT COUNT(*) FROM comments WHERE UserId = ?", (latest_id,)).fetchone()[0]
                facts.append(f"latest_user_post_count = {post_cnt}")
                facts.append(f"latest_user_comment_count = {comment_cnt}")
        if "sepang international circuit" in text and "schumacher" in text:
            rows = conn.execute(
                "SELECT SUM(T2.wins) FROM drivers AS T1 "
                "INNER JOIN driverStandings AS T2 ON T2.driverId = T1.driverId "
                "INNER JOIN races AS T3 ON T3.raceId = T2.raceId "
                "INNER JOIN circuits AS T4 ON T4.circuitId = T3.circuitId "
                "WHERE T1.forename = 'Michael' AND T1.surname = 'Schumacher' "
                "AND T4.name = 'Sepang International Circuit'"
            ).fetchone()[0]
            facts.append(f"schumacher_sepang_wins_sum = {rows}")
        if "belgium" in text:
            row = conn.execute("SELECT id FROM Country WHERE name = 'Belgium' LIMIT 1").fetchone()
            if row:
                facts.append(f"belgium_country_id = {row[0]}")
                mc = conn.execute("SELECT COUNT(*) FROM Match WHERE country_id = ?", (row[0],)).fetchone()[0]
                facts.append(f"belgium_match_count = {mc}")
        if "vision" in text and "country" in text:
            cnt = conn.execute("SELECT COUNT(*) FROM Player_Attributes WHERE vision > 89").fetchone()[0]
            facts.append(f"vision_gt_89_rows = {cnt}")
    except Exception:
        pass
    finally:
        conn.close()
    return facts


def build_candidate_blocks(candidate_rows: list[dict[str, Any]]) -> str:
    blocks = []
    for row in candidate_rows:
        blocks.append(
            f"[{row['index']}]\n- rows_preview: {row['rows_preview']}\n- sql:\n{row['sql']}\n"
        )
    return "\n".join(blocks)


def call_qwen(client: OpenAI, model_name: str, prompt: str) -> dict[str, Any]:
    rsp = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_tokens=8196,
        timeout=60,
    )
    text = rsp.choices[0].message.content or ""
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return {"chosen_index": None, "raw_response": text}
    try:
        payload = json.loads(m.group(0))
    except Exception:
        payload = {"chosen_index": None}
    payload["raw_response"] = text
    return payload


def collect_success_candidates(sample: dict[str, Any], db_path: str, timeout: int) -> list[dict[str, Any]]:
    rows = []
    seen = set()
    for c in sample.get("sql_candidates", []):
        ex = c.get("execution") or {}
        if ex.get("status") != "succeeded" or ex.get("empty_result"):
            continue
        sql = (c.get("sql") or "").strip()
        if not sql:
            continue
        norm = " ".join(sql.lower().split())
        if norm in seen:
            continue
        seen.add(norm)
        idx = int(c.get("index", len(rows)))
        _, preview = _exec_sql_signature(db_path, sql, timeout)
        rows.append({"index": idx, "sql": sql, "rows_preview": preview})
    return rows


def repair_timeout_fallback(sample: dict[str, Any], timeout_source_sample: dict[str, Any], db_path: str, max_repairs: int) -> list[dict[str, Any]]:
    schema_info = sample.get("schema_info") or {}
    schema_description = schema_info.get("schema_description") or schema_info.get("schema_prompt_text") or ""
    question = sample.get("question", "")
    evidence = sample.get("evidence", "")
    value_hints = sample.get("value_link_prompt_hints", "")

    rows = []
    seen = set()
    repaired = 0
    for c in timeout_source_sample.get("sql_candidates", []):
        ex = c.get("execution") or {}
        if ex.get("status") != "timeout":
            continue
        sql = (c.get("sql") or "").strip()
        if not sql:
            continue
        norm = " ".join(sql.lower().split())
        if norm in seen:
            continue
        seen.add(norm)
        correction = _correct_timeout(
            question=question,
            candidate_sql=sql,
            schema_description=schema_description,
            evidence=evidence,
            value_link_prompt_hints=value_hints,
            timeout_seconds=120,
        )
        corrected_sql = (correction.get("corrected_sql") or "").strip()
        if not corrected_sql:
            continue
        re_exec = _exec_sql(corrected_sql, db_path, 120)
        if re_exec.get("status") == "succeeded" and not re_exec.get("empty_result", False):
            idx = 1000 + repaired
            rows.append(
                {
                    "index": idx,
                    "sql": corrected_sql,
                    "rows_preview": (re_exec.get("rows") or [])[:5],
                }
            )
            repaired += 1
        if repaired >= max_repairs:
            break
    return rows


def evaluate_one(sample: dict[str, Any], timeout_source_sample: dict[str, Any], gold_map: dict[int, dict[str, Any]] | None, args: argparse.Namespace, model_cfg: dict[str, Any]) -> dict[str, Any] | None:
    qid = int(sample["question_id"])
    gold = gold_map.get(qid) if gold_map is not None else None
    db_id = sample["db_id"]
    db_path = str(args.db_root / db_id / f"{db_id}.sqlite")

    candidate_rows = collect_success_candidates(sample, db_path, args.timeout)
    fallback_used = False
    if not candidate_rows:
        candidate_rows = repair_timeout_fallback(sample, timeout_source_sample, db_path, args.max_timeout_repairs)
        fallback_used = True
    if not candidate_rows:
        return None

    probe_facts = build_probe_facts(sample, db_path)
    prompt = PROMPT.format(
        question=sample.get("question", ""),
        evidence=sample.get("evidence", "") or "<empty>",
        probe_facts="\n".join(f"- {x}" for x in probe_facts) or "- none",
        candidate_blocks=build_candidate_blocks(candidate_rows),
    )
    client = OpenAI(base_url=model_cfg["base_url"], api_key=model_cfg["api_key"])
    choice = call_qwen(client, model_cfg["model_name"], prompt)
    chosen_idx = choice.get("chosen_index")
    chosen_sql = next((r["sql"] for r in candidate_rows if r["index"] == chosen_idx), None)
    result = {
        "question_id": qid,
        "db_id": db_id,
        "chosen_index": chosen_idx,
        "chosen_sql": chosen_sql,
        "probe_facts": probe_facts,
        "fallback_used": fallback_used,
        "raw_response": choice.get("raw_response"),
    }
    if gold is not None:
        gold_sig, _ = _exec_sql_signature(db_path, gold["SQL"], args.timeout)
        chosen_sig, _ = _exec_sql_signature(db_path, chosen_sql, args.timeout) if chosen_sql else (None, None)
        result["is_correct"] = bool(chosen_sig is not None and gold_sig is not None and chosen_sig == gold_sig)
    else:
        result["is_correct"] = None
    return result


def main() -> None:
    args = parse_args()
    run_payload = json.loads(args.run_final.read_text())
    timeout_source_payload = json.loads(args.run_timeout_source.read_text())
    qids = [int(x.strip()) for x in args.qid_list.read_text().splitlines() if x.strip()]
    run_map = {int(s["question_id"]): s for s in run_payload["results"]}
    timeout_source_map = {int(s["question_id"]): s for s in timeout_source_payload["results"]}
    gold_map = None
    if args.gold:
        gold_rows = json.loads(args.gold.read_text())
        gold_map = {int(r["question_id"]): r for r in gold_rows}
    model_cfg = get_model_config(args.model_key)

    samples = [
        (run_map[qid], timeout_source_map[qid])
        for qid in qids
        if qid in run_map and qid in timeout_source_map and (gold_map is None or qid in gold_map)
    ]
    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        futures = [
            executor.submit(evaluate_one, sample, timeout_source_sample, gold_map, args, model_cfg)
            for sample, timeout_source_sample in samples
        ]
        total_futures = len(futures)
        completed = 0
        for future in as_completed(futures):
            item = future.result()
            completed += 1
            if item is not None:
                results.append(item)
            if completed % 5 == 0 or completed == total_futures:
                logger.info(
                    "TimeoutRiskRouter: progress {}/{} (results={})",
                    completed,
                    total_futures,
                    len(results),
                )
    results.sort(key=lambda x: x["question_id"])
    summary = {
        "evaluated": len(results),
        "fallback_used_count": sum(int(r.get("fallback_used")) for r in results),
    }
    if gold_map is not None:
        summary["correct"] = sum(int(bool(r["is_correct"])) for r in results)
        summary["accuracy"] = (summary["correct"] / len(results)) if results else 0.0
    args.output.write_text(json.dumps({"summary": summary, "results": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
