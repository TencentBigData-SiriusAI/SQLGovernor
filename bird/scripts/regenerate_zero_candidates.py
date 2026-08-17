#!/usr/bin/env python3
"""Regenerate candidates for zero-candidate samples after rule filtering.

This script targets samples whose `sql_candidates` became empty in a filtered
run file (e.g., `experiments/dev_run_0224_new`), and regenerates SQL candidates
by reusing SQL generation prompts plus additional feedback:

1) previously removed (bad) SQL examples for the sample
2) deduplicated filtering rules that those bad SQLs violated

It writes a new run JSON file and does not modify inputs.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from openai import OpenAI

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import Settings
from config.model_config import get_model_config
from src.tools.sql_projection_rules import apply_projection_rules
from src.tools.sql_validator import validate_sql_schema, validate_sql_syntax
from src.utils.sql_text import clean_sql_output


SYSTEM_PROMPT_PRIMARY = (
    "You are a data science expert. Below, you are provided with a database schema "
    "and a natural language question. Your task is to understand the schema and "
    "generate a valid SQL query to answer the question."
)

PROMPT_TEMPLATE_PRIMARY = """Database Engine:
SQLite

Database Schema:
{schema}
This schema describes the database’s structure, including tables, columns, primary keys, foreign keys, and any relevant relationships or constraints.

Question:
{question_with_knowledge}

Instructions:
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more, and never use `SELECT *`.
- The generated query should return all of the information asked in the question without any missing or extra information.
- Before generating the final SQL query, please think through the steps of how to write the query.
{dynamic_instructions}


Output Format:
In your answer, please enclose the generated SQL query in a code block:
```sql
-- Your SQL query
```

Take a deep breath and think step by step to find the correct SQL query."""

AGGREGATE_KEYWORDS = ["how many", "average", "ratio", "percentage", "percent"]
EXTREME_KEYWORDS = ["top", "highest", "lowest", "rank", "order"]
CALIFORNIA_SCHOOLS_DB_ID = "california_schools"
ROUND_KEYWORDS = ["round", "rounded", "decimal", "decimals", "nearest", "precision"]
DECIMAL_REGEXES = [
    re.compile(r"round(?:ed)?(?: to)?\s*(\d+)\s*decimal"),
    re.compile(r"(\d+)\s*decimal\s*places"),
    re.compile(r"(\d+)\s*decimals"),
]
DECIMAL_WORD_REGEX = re.compile(
    r"(one|two|three|four|five|six|seven|eight|nine|ten)\s+decimal(?:\s+places?)?",
    re.IGNORECASE,
)
NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}


RuleCond = Tuple[str, bool]
ParsedRule = Tuple[str, List[RuleCond]]

QUESTION_PATTERNS = {
    "q_how_many": r"\bhow many\b",
    "q_how_often": r"\bhow often\b",
    "q_average": r"\baverage|avg|mean\b",
    "q_least": r"\bleast|lowest\b",
    "q_most": r"\bmost|highest\b",
    "q_top": r"\btop\s*\d*\b",
    "q_earliest_latest": r"\bearliest|latest\b",
    "q_first_last": r"\bfirst|last\b",
    "q_last_to": r"\blast\s+to\b",
    "q_first_time": r"\bfirst time\b",
    "q_edit": r"\bedit|edited\b",
    "q_created": r"\bcreated|creation\b",
    "q_consumption": r"\bconsumption\b",
    "q_salary": r"\bsalary\b",
    "q_amount": r"\bamount\b",
    "q_rate": r"\brate\b",
    "q_list_cue": r"\blist|show|give|indicate|write down|name\b",
    "q_entity_who_which": r"\bwho|which\b",
    "q_at_least": r"\bat least\b",
    "q_after_before": r"\bafter|before\b",
}

SQL_PATTERNS = {
    "s_has_order": r"\border\s+by\b",
    "s_has_limit": r"\blimit\b",
    "s_has_group": r"\bgroup\s+by\b",
    "s_has_having": r"\bhaving\b",
    "s_has_count": r"\bcount\s*\(",
    "s_has_sum": r"\bsum\s*\(",
    "s_has_avg": r"\bavg\s*\(",
    "s_has_min": r"\bmin\s*\(",
    "s_has_max": r"\bmax\s*\(",
    "s_has_rank_fn": r"\brow_number\s*\(|\brank\s*\(|\bdense_rank\s*\(",
    "s_has_rank_eq1": r"\brank\s*=\s*1\b",
    "s_has_where": r"\bwhere\b",
    "s_has_join": r"\bjoin\b",
    "s_has_subq": r"\(\s*select\b",
    "s_has_distinct": r"\bdistinct\b",
}


@dataclass
class RegenConfig:
    candidate_count: int
    batch_size: int
    max_rounds: int
    temperature: float
    max_tokens: int
    timeout: int
    max_retries: int
    retry_backoff: float
    max_bad_sql_examples: int


def _build_dynamic_instructions(question: str, evidence: str, db_id: str | None) -> str:
    question_text = (question or "").strip().lower()
    evidence_text = (evidence or "").strip().lower()
    combined_text = f"{question_text} {evidence_text}".strip()

    def _extract_decimal_places(text: str) -> int | None:
        for regex in DECIMAL_REGEXES:
            match = regex.search(text)
            if match:
                try:
                    return int(match.group(1))
                except (IndexError, ValueError):
                    continue
        word_match = DECIMAL_WORD_REGEX.search(text)
        if word_match:
            return NUMBER_WORDS.get(word_match.group(1).lower())
        return None

    requires_aggregate = any(keyword in combined_text for keyword in AGGREGATE_KEYWORDS)
    requires_extreme = any(keyword in combined_text for keyword in EXTREME_KEYWORDS)
    allows_avg_field = (
        (db_id or "").lower() == CALIFORNIA_SCHOOLS_DB_ID and "average score" in question_text
    )
    decimal_requirement = _extract_decimal_places(combined_text)
    requires_rounding = bool(
        decimal_requirement is not None
        and any(keyword in combined_text for keyword in ROUND_KEYWORDS)
    )

    lines: List[str] = []
    if requires_aggregate:
        lines.append(
            "- Because the question mentions counts/averages/ratios, ensure SELECT includes the corresponding expression."
        )
    if requires_extreme:
        lines.append(
            "- If highest/lowest/top is requested, use ORDER BY ... LIMIT (or RANK()) to pick the extremum."
        )
    if allows_avg_field:
        lines.append(
            "- For California schools, AvgScrMath/AvgScrRead/AvgScrWrite can directly represent average SAT scores."
        )
    if requires_rounding and decimal_requirement:
        lines.append(
            f"- Round requested metrics to {decimal_requirement} decimal places with ROUND(..., {decimal_requirement})."
        )
    if not lines:
        return ""
    return "\n" + "\n".join(lines)


def _call_sql_generation_api(
    client: OpenAI,
    model_name: str,
    messages: List[Dict[str, Any]],
    temperature: float,
    max_tokens: int,
    n: int,
    timeout: int,
    max_retries: int,
    retry_backoff: float,
    question_id: Any,
) -> List[str]:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                n=n,
                timeout=timeout,
            )
            return [choice.message.content or "" for choice in response.choices]
        except Exception as exc:  # pragma: no cover
            last_error = exc
            wait_time = retry_backoff * attempt
            print(
                f"[warn] question_id={question_id} generation failed attempt={attempt}/{max_retries}: {exc}; retry in {wait_time:.1f}s"
            )
            time.sleep(wait_time)
    raise RuntimeError(f"SQL generation API failed for question_id={question_id}: {last_error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Regenerate zero-candidate SQL samples.")
    parser.add_argument(
        "--source-run",
        default=str(PROJECT_ROOT / "experiments" / "dev_run_0224"),
        help="Original unfiltered run JSON path.",
    )
    parser.add_argument(
        "--filtered-run",
        default=str(PROJECT_ROOT / "experiments" / "dev_run_0224_new"),
        help="Filtered run JSON path (contains zero-candidate samples).",
    )
    parser.add_argument(
        "--rules-file",
        default=str(PROJECT_ROOT / "experiments" / "dev_run_0224_new.rules.txt"),
        help="Rules text file used for filtering (one rule per line).",
    )
    parser.add_argument(
        "--output-run",
        default=str(PROJECT_ROOT / "experiments" / "dev_run_0224_new_regen10"),
        help="Output path for regenerated run JSON.",
    )
    parser.add_argument(
        "--qid-list",
        default="",
        help="Optional comma-separated qids to regenerate. Defaults to all zero-candidate qids in filtered-run.",
    )
    parser.add_argument(
        "--model-key",
        default="qwen3-235b",
        help="Model key from config/model_config.py",
    )
    parser.add_argument("--candidate-count", type=int, default=32)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-rounds", type=int, default=6)
    parser.add_argument("--temperature", type=float, default=Settings.SQL_GENERATION_PRIMARY_TEMPERATURE)
    parser.add_argument("--max-tokens", type=int, default=Settings.MAX_TOKENS)
    parser.add_argument("--timeout", type=int, default=Settings.SQL_GENERATION_REQUEST_TIMEOUT)
    parser.add_argument("--max-retries", type=int, default=Settings.SQL_GENERATION_MAX_RETRIES)
    parser.add_argument("--retry-backoff", type=float, default=Settings.SQL_GENERATION_RETRY_BACKOFF)
    parser.add_argument("--max-bad-sql-examples", type=int, default=12)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print target qids and planned actions; do not call model API.",
    )
    return parser.parse_args()


def parse_rules(lines: Sequence[str]) -> List[ParsedRule]:
    parsed: List[ParsedRule] = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        conds: List[RuleCond] = []
        for part in line.split("&"):
            key, val = part.strip().split("=")
            conds.append((key.strip(), val.strip() == "T"))
        parsed.append((line, conds))
    return parsed


def compile_patterns(patterns: Dict[str, str]) -> Dict[str, re.Pattern[str]]:
    return {k: re.compile(v, re.I) for k, v in patterns.items()}


def extract_features(
    question: str,
    sql: str,
    q_re: Dict[str, re.Pattern[str]],
    s_re: Dict[str, re.Pattern[str]],
) -> Dict[str, bool]:
    q = question.lower()
    s = sql.lower()
    feats: Dict[str, bool] = {}
    for key, pat in q_re.items():
        feats[key] = bool(pat.search(q))
    for key, pat in s_re.items():
        feats[key] = bool(pat.search(s))
    feats["s_order_limit"] = feats["s_has_order"] and feats["s_has_limit"]
    feats["s_count_or_sum"] = feats["s_has_count"] or feats["s_has_sum"]
    feats["s_avg_or_sum_count"] = feats["s_has_avg"] or bool(
        re.search(r"\bsum\s*\([^)]*\)\s*/\s*count\s*\(", s)
    )
    feats["s_any_rank_mech"] = (
        feats["s_has_order"]
        or feats["s_has_rank_fn"]
        or feats["s_has_rank_eq1"]
        or feats["s_has_min"]
        or feats["s_has_max"]
    )
    return feats


def hit_rule(feats: Dict[str, bool], conds: Sequence[RuleCond]) -> bool:
    return all(feats.get(k, False) == v for k, v in conds)


def normalize_sql(sql: str) -> str:
    return " ".join((sql or "").strip().split()).lower()


def build_feedback_block(
    question: str,
    removed_sqls: Sequence[str],
    matched_rules: Sequence[str],
    max_bad_sql_examples: int,
) -> str:
    blocks: List[str] = []
    reasons = infer_error_reasons(question, removed_sqls, matched_rules)
    if reasons:
        blocks.append("Inferred error reasons from removed SQLs:")
        for idx, reason in enumerate(reasons, 1):
            blocks.append(f"{idx}. {reason}")
        blocks.append("")

    if matched_rules:
        blocks.append("Targeted violated rule patterns (deduplicated):")
        for idx, rule in enumerate(matched_rules, 1):
            blocks.append(f"{idx}. {rule}")

    bad_examples = [s for s in removed_sqls if s.strip()][:max_bad_sql_examples]
    if bad_examples:
        blocks.append("")
        blocks.append("Previously rejected SQL examples (do not repeat these mistakes):")
        for idx, sql in enumerate(bad_examples, 1):
            blocks.append(f"-- bad_sql_{idx}")
            blocks.append("```sql")
            blocks.append(sql.strip())
            blocks.append("```")

    blocks.append("")
    blocks.append("Hard constraints for this retry:")
    blocks.append("- Do NOT produce SQL that matches any rejected rule pattern above.")
    blocks.append("- Do NOT repeat the listed bad SQL structures.")
    blocks.append("- Return exactly the requested fields with no extras.")
    return "\n".join(blocks).strip()


def infer_error_reasons(
    question: str, removed_sqls: Sequence[str], matched_rules: Sequence[str]
) -> List[str]:
    """Infer coarse error reasons from bad SQLs and matched rules."""
    q = (question or "").lower()
    sql_joined = "\n".join(removed_sqls).lower()
    reasons: List[str] = []

    def add(reason: str) -> None:
        if reason not in reasons:
            reasons.append(reason)

    # Rule-driven reasons
    for rule in matched_rules:
        r = rule.lower()
        if "q_average" in r or "q_how_many" in r or "s_avg_or_sum_count" in r or "s_count_or_sum" in r:
            add("aggregation/metric mismatch (count/avg/sum semantics likely wrong)")
        if "q_most" in r or "q_least" in r or "q_first_last" in r or "q_earliest_latest" in r:
            add("extreme/ranking logic mismatch (top/least/earliest selection likely wrong)")
        if "s_has_join=f" in r:
            add("join path likely missing or incorrect")
        if "s_has_where=f" in r:
            add("filter predicate likely missing or under-specified")
        if "q_list_cue=f" in r and ("s_has_group=t" in r or "s_has_max=t" in r):
            add("projection likely drifts from requested output fields")

    # SQL-pattern fallback reasons
    if ("how many" in q or "how often" in q or "average" in q) and not (
        re.search(r"\bcount\s*\(", sql_joined)
        or re.search(r"\bavg\s*\(", sql_joined)
        or re.search(r"\bsum\s*\(", sql_joined)
    ):
        add("missing explicit aggregate expression for requested metric")

    if any(k in q for k in ["top", "highest", "lowest", "earliest", "latest", "first", "last"]) and not (
        "order by" in sql_joined or re.search(r"\b(min|max)\s*\(", sql_joined)
    ):
        add("missing robust ranking/extreme mechanism")

    if re.search(r"\bselect\b", sql_joined) and not re.search(r"\bwhere\b", sql_joined):
        add("risk of insufficient filtering constraints")

    return reasons


def build_prompt(item: Dict[str, Any], feedback_block: str) -> str:
    question = item.get("question", "").strip()
    evidence = (item.get("evidence") or "").strip()
    schema_info = item.get("schema_info") or {}
    schema = (schema_info.get("schema_description") or "").strip() or "(schema unavailable)"
    value_hints = item.get("value_link_prompt_hints")
    if not isinstance(value_hints, str) or not value_hints.strip():
        value_hints = "(No high-confidence value hints retrieved.)"
    question_with_knowledge = f"{evidence}\n{question}".strip() if evidence else question
    dynamic = _build_dynamic_instructions(question=question, evidence=evidence, db_id=item.get("db_id"))
    if dynamic:
        dynamic = dynamic + "\n- Use the additional retry feedback below to avoid prior mistakes."
    else:
        dynamic = "\n- Use the additional retry feedback below to avoid prior mistakes."
    dynamic = dynamic + "\n\n" + feedback_block

    return PROMPT_TEMPLATE_PRIMARY.format(
        schema=schema,
        question_with_knowledge=question_with_knowledge,
        value_link_hints=value_hints,
        few_shot_examples="(No contextual examples available.)",
        dynamic_instructions=dynamic,
    )


def generate_for_item(
    item: Dict[str, Any],
    prompt: str,
    cfg: RegenConfig,
    client: OpenAI,
    model_name: str,
) -> List[Dict[str, Any]]:
    db_id = item.get("db_id")
    schema_info = item.get("schema_info") or {}
    question = item.get("question", "")
    question_id = item.get("question_id")
    database_path = str(Settings.get_database_path(db_id))

    api_messages = [
        {"role": "system", "content": SYSTEM_PROMPT_PRIMARY},
        {"role": "user", "content": prompt},
    ]

    seen_norm: set[str] = set()
    candidates: List[Dict[str, Any]] = []
    duplicate_pool: List[Dict[str, Any]] = []
    raw_idx = 0

    for _round in range(cfg.max_rounds):
        if len(candidates) >= cfg.candidate_count:
            break
        need = cfg.candidate_count - len(candidates)
        ask_n = min(cfg.batch_size, need)

        responses = _call_sql_generation_api(
            client=client,
            model_name=model_name,
            messages=api_messages,
            temperature=cfg.temperature,
            max_tokens=cfg.max_tokens,
            n=ask_n,
            timeout=cfg.timeout,
            max_retries=cfg.max_retries,
            retry_backoff=cfg.retry_backoff,
            question_id=question_id,
        )

        for raw in responses:
            raw_idx += 1
            sql = clean_sql_output(raw or "")
            if Settings.SQL_PROJECTION_RULES_ENABLED and sql:
                adjusted_sql, _adjustments = apply_projection_rules(sql, question)
                sql = adjusted_sql

            norm = normalize_sql(sql)
            if not norm:
                continue

            syntax_errors = validate_sql_syntax(sql) if sql else ["EMPTY_SQL"]
            schema_errors: List[str] = []
            if not syntax_errors:
                schema_errors = validate_sql_schema(sql, schema_info, database_path)
            errors = syntax_errors + schema_errors

            candidate_obj = {
                "index": len(candidates),
                "sql": sql,
                "source_group": "regen_retry",
                "source_model": model_name,
                "source": "generation_retry",
                "vote_count": 1,
                "result_vote_score": None,
                "validation": {
                    "status": "ok" if not errors else "error",
                    "errors": errors,
                    "warnings": [],
                },
                "execution": {
                    "status": "not_executed",
                    "success": None,
                    "rows": None,
                    "error": None,
                    "elapsed": None,
                },
                "raw_response": raw,
            }
            if norm in seen_norm:
                duplicate_pool.append(candidate_obj)
                continue
            seen_norm.add(norm)
            candidates.append(candidate_obj)

            if len(candidates) >= cfg.candidate_count:
                break

    # Fill up to target count if model outputs are highly duplicated.
    fill_source: List[Dict[str, Any]] = duplicate_pool if duplicate_pool else candidates
    fill_idx = 0
    while len(candidates) < cfg.candidate_count and fill_source:
        base = copy.deepcopy(fill_source[fill_idx % len(fill_source)])
        base["index"] = len(candidates)
        base["source"] = "generation_retry_fill"
        candidates.append(base)
        fill_idx += 1

    return candidates


def parse_qids(qid_text: str) -> List[int]:
    if not qid_text.strip():
        return []
    out: List[int] = []
    for part in qid_text.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def main() -> None:
    args = parse_args()

    source_run_path = Path(args.source_run)
    filtered_run_path = Path(args.filtered_run)
    rules_path = Path(args.rules_file)
    output_path = Path(args.output_run)

    source_data = json.loads(source_run_path.read_text(encoding="utf-8"))
    filtered_data = json.loads(filtered_run_path.read_text(encoding="utf-8"))
    rules = parse_rules(rules_path.read_text(encoding="utf-8").splitlines())
    if not rules:
        raise ValueError(f"No valid rules found in {rules_path}")

    source_by_qid = {item["question_id"]: item for item in source_data.get("results", [])}
    filtered_by_qid = {item["question_id"]: item for item in filtered_data.get("results", [])}

    qids = parse_qids(args.qid_list)
    if not qids:
        qids = [
            item["question_id"]
            for item in filtered_data.get("results", [])
            if len(item.get("sql_candidates", [])) == 0
        ]
    qids = sorted(set(qids))

    q_re = compile_patterns(QUESTION_PATTERNS)
    s_re = compile_patterns(SQL_PATTERNS)

    print(f"target_qids={qids}")
    if args.dry_run:
        print("dry_run=true")
        return

    cfg = RegenConfig(
        candidate_count=args.candidate_count,
        batch_size=args.batch_size,
        max_rounds=args.max_rounds,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout=args.timeout,
        max_retries=args.max_retries,
        retry_backoff=args.retry_backoff,
        max_bad_sql_examples=args.max_bad_sql_examples,
    )

    model_cfg = get_model_config(args.model_key)
    client = OpenAI(base_url=model_cfg["base_url"], api_key=model_cfg["api_key"])
    model_name = model_cfg["model_name"]

    output_data = copy.deepcopy(filtered_data)
    out_by_qid = {item["question_id"]: item for item in output_data.get("results", [])}

    regen_stats = []
    for qid in qids:
        src = source_by_qid.get(qid)
        dst = out_by_qid.get(qid)
        if not src or not dst:
            regen_stats.append((qid, "missing_sample", 0))
            continue

        src_candidates = src.get("sql_candidates", [])
        dst_candidates = dst.get("sql_candidates", [])
        if dst_candidates:
            regen_stats.append((qid, "skip_non_empty", len(dst_candidates)))
            continue

        # For zero-candidate samples, all source candidates are treated as removed bad SQL.
        removed_sqls = [c.get("sql", "") for c in src_candidates if c.get("sql")]
        # Deduplicate while preserving order.
        dedup_removed: List[str] = []
        seen_bad: set[str] = set()
        for sql in removed_sqls:
            norm = normalize_sql(sql)
            if not norm or norm in seen_bad:
                continue
            seen_bad.add(norm)
            dedup_removed.append(sql)

        # Collect matched rule patterns from removed SQLs.
        matched_rules: List[str] = []
        matched_seen: set[str] = set()
        question = src.get("question", "")
        for bad_sql in dedup_removed:
            f = extract_features(question, bad_sql, q_re, s_re)
            for rule_text, conds in rules:
                if hit_rule(f, conds) and rule_text not in matched_seen:
                    matched_seen.add(rule_text)
                    matched_rules.append(rule_text)

        feedback_block = build_feedback_block(
            question=question,
            removed_sqls=dedup_removed,
            matched_rules=matched_rules,
            max_bad_sql_examples=cfg.max_bad_sql_examples,
        )
        prompt = build_prompt(src, feedback_block)
        new_candidates = generate_for_item(
            item=src,
            prompt=prompt,
            cfg=cfg,
            client=client,
            model_name=model_name,
        )

        dst["sql_candidates"] = new_candidates
        dst["regen_info"] = {
            "regenerated_at": datetime.now(timezone.utc).isoformat(),
            "model_key": args.model_key,
            "model_name": model_name,
            "preset_rules_file": str(rules_path),
            "used_removed_sql_count": len(dedup_removed),
            "used_rule_feedback_count": len(matched_rules),
            "generated_candidate_count": len(new_candidates),
        }
        regen_stats.append((qid, "regenerated", len(new_candidates)))
        print(
            f"qid={qid} regenerated candidates={len(new_candidates)} "
            f"bad_sql={len(dedup_removed)} rules={len(matched_rules)}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"output={output_path}")
    print("summary:")
    for qid, status, count in regen_stats:
        print(f"  qid={qid}\t{status}\tcount={count}")


if __name__ == "__main__":
    main()
