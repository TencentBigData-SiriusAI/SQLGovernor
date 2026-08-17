#!/usr/bin/env python3
"""Filter SQL candidates in a run JSON file with rule-based predicates.

Default preset (`qes_30`) uses the 30-rule package based on
question + evidence + sql features.

Optional GT signature guard uses train/GT rows as a whitelist: if a candidate
hits a rule but its full feature signature has already appeared in GT for that
same rule, the candidate is kept instead of removed.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple


RuleCond = Tuple[str, bool]
ParsedRule = List[RuleCond]
FeatureSignature = Tuple[bool, ...]


RULES_QES_30 = [
    "q_consumption=T & s_any_rank_mech=T & s_count_or_sum=F",
    "q_entity_who_which=T & s_has_max=T & s_has_subq=F",
    "q_entity_who_which=F & s_avg_or_sum_count=T & s_has_order=T",
    "q_least=T & s_has_join=F & s_has_where=F",
    "q_amount=T & q_average=T & s_has_join=F",
    "q_average=T & q_least=T & s_has_where=F",
    "q_average=F & q_earliest_latest=T & s_has_limit=F",
    "q_created=F & s_has_group=T & s_has_min=T",
    "q_entity_who_which=F & q_first_last=T & s_has_max=T",
    "q_list_cue=F & s_any_rank_mech=F & s_has_limit=T",
    "q_after_before=T & s_has_group=T & s_has_having=F",
    "q_consumption=T & s_has_order=T & s_has_subq=T",
    "q_earliest_latest=T & q_list_cue=F & s_has_join=F",
    "q_list_cue=F & s_has_group=T & s_has_max=T",
    "q_most=T & s_avg_or_sum_count=T & s_has_group=T",
    "q_how_many=T & s_has_join=F & s_has_limit=T",
    "q_edit=T & s_has_count=T & s_has_distinct=F",
    "e_has_formula=T & q_after_before=T & s_has_avg=T",
    "e_has_join_hint=T & q_after_before=T & s_has_avg=T",
    "qe_has_agg_intent=F & q_created=T & s_has_sum=T",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_subq=T",
    "e_has_filter_literal=F & q_top=T & s_has_distinct=T",
    "qe_has_filter_intent=F & q_top=T & s_has_distinct=T",
    "e_has_agg_hint=F & q_first_last=T & s_avg_or_sum_count=T",
    "e_has_agg_hint=F & q_first_last=T & s_has_avg=T",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_limit=F",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_max=T",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_order=F",
    "e_has_join_hint=T & q_earliest_latest=T & s_order_limit=F",
    "qe_has_agg_intent=F & q_consumption=T & s_has_subq=T",
]

RULES_REFINED_DEPTH1 = [
    "q_consumption=T & s_any_rank_mech=T & s_count_or_sum=F",
    "q_entity_who_which=T & s_has_max=T & s_has_subq=F & q_amount=T",
    "q_entity_who_which=F & s_avg_or_sum_count=T & s_has_order=T & q_created=T",
    "q_least=T & s_has_join=F & s_has_where=F & q_average=T",
    "q_amount=T & q_average=T & s_has_join=F & e_has_agg_hint=F",
    "q_average=T & q_least=T & s_has_where=F & e_has_formula=F",
    "q_average=F & q_earliest_latest=T & s_has_limit=F & q_created=T",
    "q_created=F & s_has_group=T & s_has_min=T & q_average=T",
    "q_entity_who_which=F & q_first_last=T & s_has_max=T & e_has_year=T",
    "q_list_cue=F & s_any_rank_mech=F & s_has_limit=T & q_how_often=T",
    "q_after_before=T & s_has_group=T & s_has_having=F & s_count_or_sum=F",
    "q_consumption=T & s_has_order=T & s_has_subq=T & e_has_join_hint=T",
    "q_earliest_latest=T & q_list_cue=F & s_has_join=F & s_has_subq=T",
    "q_list_cue=F & s_has_group=T & s_has_max=T & e_has_formula=T",
    "q_most=T & s_avg_or_sum_count=T & s_has_group=T & q_rate=T",
    "q_how_many=T & s_has_join=F & s_has_limit=T & q_top=T",
    "q_edit=T & s_has_count=T & s_has_distinct=F & q_first_last=T",
    "e_has_formula=T & q_after_before=T & s_has_avg=T",
    "e_has_join_hint=T & q_after_before=T & s_has_avg=T",
    "qe_has_agg_intent=F & q_created=T & s_has_sum=T",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_subq=T",
    "e_has_filter_literal=F & q_top=T & s_has_distinct=T",
    "qe_has_filter_intent=F & q_top=T & s_has_distinct=T",
    "e_has_agg_hint=F & q_first_last=T & s_avg_or_sum_count=T",
    "e_has_agg_hint=F & q_first_last=T & s_has_avg=T",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_limit=F",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_max=T",
    "e_has_join_hint=T & q_earliest_latest=T & s_has_order=F",
    "e_has_join_hint=T & q_earliest_latest=T & s_order_limit=F",
    "qe_has_agg_intent=F & q_consumption=T & s_has_subq=T",
]

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

EVIDENCE_PATTERNS = {
    "e_has_year": r"\b(19\d{2}|20\d{2})\b|\byear\b",
    "e_has_formula": r"\b(rate|ratio|per|/|divide|percent|%)\b",
    "e_has_compare": r"\b(after|before|greater than|less than|at least|at most|more than|fewer than)\b",
    "e_has_superlative": r"\b(top|highest|lowest|max|min|first|last|earliest|latest)\b",
    "e_has_agg_hint": r"\b(count|sum|avg|average|mean|total)\b",
    "e_has_join_hint": r"\b(table|join|foreign key|id|belongs to|associated)\b",
    "e_has_filter_literal": r"[\"'`].+?[\"'`]|\b[A-Z][A-Za-z0-9_]{2,}\b",
}

DERIVED_FEATURE_KEYS = (
    "s_order_limit",
    "s_count_or_sum",
    "s_avg_or_sum_count",
    "s_any_rank_mech",
    "qe_has_agg_intent",
    "qe_has_filter_intent",
)

FEATURE_KEY_ORDER = tuple(
    sorted(
        set(QUESTION_PATTERNS)
        | set(SQL_PATTERNS)
        | set(EVIDENCE_PATTERNS)
        | set(DERIVED_FEATURE_KEYS)
    )
)

ROW_SQL_KEYS = ("SQL", "sql", "query", "gt_sql")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter sql_candidates in run JSON by rule predicates."
    )
    parser.add_argument("--input", required=True, help="Input run JSON path")
    parser.add_argument("--output", required=True, help="Output run JSON path")
    parser.add_argument(
        "--preset",
        choices=["qes_30", "refined_rules_depth1"],
        default="qes_30",
        help="Built-in rules preset",
    )
    parser.add_argument(
        "--rules-file",
        default=None,
        help="Optional text file (one rule per line) to override preset",
    )
    parser.add_argument(
        "--export-rules",
        default=None,
        help="Optional output path to dump the actual rules used",
    )
    parser.add_argument(
        "--only-fail-qids",
        default=None,
        help="Optional pass_failures txt path; if set, only filter those question_id blocks",
    )
    parser.add_argument(
        "--train-gt-json",
        default=None,
        help=(
            "Optional GT/train JSON path. If set, candidates whose feature signature "
            "matches a GT signature for the same rule will be kept."
        ),
    )
    return parser.parse_args()


def parse_rules(lines: Sequence[str]) -> List[Tuple[str, ParsedRule]]:
    parsed: List[Tuple[str, ParsedRule]] = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        conds: ParsedRule = []
        for part in line.split("&"):
            key, val = part.strip().split("=")
            conds.append((key.strip(), val.strip() == "T"))
        parsed.append((line, conds))
    return parsed


def read_fail_qids(path: Path) -> set[int]:
    text = path.read_text(encoding="utf-8")
    return set(int(x) for x in re.findall(r"question_id=(\d+)\s+db_id=", text))


def build_rule_source(args: argparse.Namespace) -> List[str]:
    if args.rules_file:
        lines = Path(args.rules_file).read_text(encoding="utf-8").splitlines()
        return lines
    if args.preset == "qes_30":
        return RULES_QES_30
    if args.preset == "refined_rules_depth1":
        return RULES_REFINED_DEPTH1
    raise ValueError(f"Unknown preset: {args.preset}")


def compile_patterns(patterns: Dict[str, str]) -> Dict[str, re.Pattern[str]]:
    return {k: re.compile(v, re.I) for k, v in patterns.items()}


def extract_features(
    question: str,
    sql: str,
    evidence: str,
    q_re: Dict[str, re.Pattern[str]],
    s_re: Dict[str, re.Pattern[str]],
    e_re: Dict[str, re.Pattern[str]],
) -> Dict[str, bool]:
    q = question.lower()
    s = sql.lower()
    e = (evidence or "").lower()
    feats: Dict[str, bool] = {}
    for key, pat in q_re.items():
        feats[key] = bool(pat.search(q))
    for key, pat in s_re.items():
        feats[key] = bool(pat.search(s))
    for key, pat in e_re.items():
        feats[key] = bool(pat.search(e))
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
    agg_intent = (
        feats.get("q_how_many", False)
        or feats.get("q_average", False)
        or feats.get("q_amount", False)
    )
    feats["qe_has_agg_intent"] = (
        agg_intent or feats.get("e_has_agg_hint", False) or feats.get("e_has_formula", False)
    )
    filter_intent = feats.get("q_after_before", False) or feats.get("q_at_least", False)
    feats["qe_has_filter_intent"] = (
        filter_intent
        or feats.get("e_has_compare", False)
        or feats.get("e_has_filter_literal", False)
    )
    return feats


def build_feature_signature(feats: Mapping[str, bool]) -> FeatureSignature:
    return tuple(bool(feats.get(key, False)) for key in FEATURE_KEY_ORDER)


def extract_row_sql(row: Mapping[str, Any]) -> str:
    for key in ROW_SQL_KEYS:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def load_gt_rows(path: Path) -> List[Mapping[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "data", "samples"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return rows
    raise ValueError(f"Unsupported GT JSON structure: {path}")


def build_gt_signature_guard(
    gt_rows: Sequence[Mapping[str, Any]],
    parsed_rules: Sequence[Tuple[str, ParsedRule]],
    q_re: Dict[str, re.Pattern[str]],
    s_re: Dict[str, re.Pattern[str]],
    e_re: Dict[str, re.Pattern[str]],
) -> Tuple[Dict[str, set[FeatureSignature]], int, Dict[str, int]]:
    guard_signatures: Dict[str, set[FeatureSignature]] = defaultdict(set)
    rule_gt_hits: Dict[str, int] = defaultdict(int)
    gt_hit_rows = 0

    for row in gt_rows:
        question = str(row.get("question", "") or "")
        evidence = str(row.get("evidence", "") or "")
        sql = extract_row_sql(row)
        if not sql:
            continue

        feats = extract_features(question, sql, evidence, q_re, s_re, e_re)
        signature = build_feature_signature(feats)
        matched = False
        for rule_text, conds in parsed_rules:
            if hit_rule(feats, conds):
                matched = True
                guard_signatures[rule_text].add(signature)
                rule_gt_hits[rule_text] += 1
        if matched:
            gt_hit_rows += 1

    return dict(guard_signatures), gt_hit_rows, dict(rule_gt_hits)


def is_candidate_guarded_by_gt(
    feats: Mapping[str, bool],
    hit_rules: Sequence[str],
    gt_guard_signatures: Mapping[str, set[FeatureSignature]],
) -> bool:
    if not hit_rules or not gt_guard_signatures:
        return False
    signature = build_feature_signature(feats)
    return any(signature in gt_guard_signatures.get(rule_text, set()) for rule_text in hit_rules)


def hit_rule(feats: Dict[str, bool], conds: ParsedRule) -> bool:
    return all(feats.get(k, False) == v for k, v in conds)


def match_rule_texts(
    feats: Mapping[str, bool],
    parsed_rules: Sequence[Tuple[str, ParsedRule]],
) -> List[str]:
    return [rule_text for rule_text, conds in parsed_rules if hit_rule(feats, conds)]


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    with input_path.open("r", encoding="utf-8") as f:
        run_data = json.load(f)

    rule_lines = build_rule_source(args)
    parsed_rules = parse_rules(rule_lines)
    if not parsed_rules:
        raise ValueError("No valid rules found.")

    if args.export_rules:
        Path(args.export_rules).write_text(
            "\n".join(line for line, _ in parsed_rules) + "\n", encoding="utf-8"
        )

    fail_qids = None
    if args.only_fail_qids:
        fail_qids = read_fail_qids(Path(args.only_fail_qids))

    q_re = compile_patterns(QUESTION_PATTERNS)
    s_re = compile_patterns(SQL_PATTERNS)
    e_re = compile_patterns(EVIDENCE_PATTERNS)

    gt_guard_signatures: Dict[str, set[FeatureSignature]] = {}
    gt_guard_rows = 0
    gt_guard_hit_rows = 0
    gt_guard_rule_hits: Dict[str, int] = {}
    if args.train_gt_json:
        gt_rows = load_gt_rows(Path(args.train_gt_json))
        gt_guard_rows = len(gt_rows)
        (
            gt_guard_signatures,
            gt_guard_hit_rows,
            gt_guard_rule_hits,
        ) = build_gt_signature_guard(gt_rows, parsed_rules, q_re, s_re, e_re)

    orig_total = 0
    new_total = 0
    removed_total = 0
    gt_guard_spared_total = 0
    affected_results = 0
    zero_candidate_results = 0
    removed_on_target_qids = 0
    removed_off_target_qids = 0
    rule_hits: Dict[str, int] = defaultdict(int)

    for item in run_data.get("results", []):
        qid = item.get("question_id")
        question = item.get("question", "")
        evidence = item.get("evidence", "")
        candidates = item.get("sql_candidates", [])
        orig_total += len(candidates)

        should_filter = True
        if fail_qids is not None:
            should_filter = qid in fail_qids

        kept = []
        removed_here = 0

        for cand in candidates:
            if not should_filter:
                kept.append(cand)
                continue
            sql = cand.get("sql", "")
            feats = extract_features(question, sql, evidence, q_re, s_re, e_re)
            hit_rules = match_rule_texts(feats, parsed_rules)
            for rule_text in hit_rules:
                rule_hits[rule_text] += 1
            if hit_rules and is_candidate_guarded_by_gt(feats, hit_rules, gt_guard_signatures):
                gt_guard_spared_total += 1
                kept.append(cand)
                continue
            if hit_rules:
                removed_here += 1
                removed_total += 1
                if fail_qids is None or qid in fail_qids:
                    removed_on_target_qids += 1
                else:
                    removed_off_target_qids += 1
            else:
                kept.append(cand)

        if removed_here > 0:
            affected_results += 1
        if len(kept) == 0:
            zero_candidate_results += 1

        item["sql_candidates"] = kept
        new_total += len(kept)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(run_data, f, ensure_ascii=False, indent=2)

    print(f"output={output_path}")
    print(f"rules={len(parsed_rules)}")
    print(f"orig_total_sql={orig_total}")
    print(f"new_total_sql={new_total}")
    print(f"removed_total_sql={removed_total}")
    print(f"gt_guard_spared_total_sql={gt_guard_spared_total}")
    print(f"affected_results={affected_results}")
    print(f"zero_candidate_results={zero_candidate_results}")
    print(f"removed_on_target_qids={removed_on_target_qids}")
    print(f"removed_off_target_qids={removed_off_target_qids}")
    if args.train_gt_json:
        print(f"gt_guard_train_json={args.train_gt_json}")
        print(f"gt_guard_rows={gt_guard_rows}")
        print(f"gt_guard_hit_rows={gt_guard_hit_rows}")
        print(f"gt_guard_rules={len(gt_guard_signatures)}")
        print("gt_guard_rule_hits:")
        for rule_text, count in sorted(
            gt_guard_rule_hits.items(), key=lambda x: x[1], reverse=True
        ):
            print(f"{count}\t{rule_text}")
    print("top_rule_hits:")
    for rule_text, count in sorted(rule_hits.items(), key=lambda x: x[1], reverse=True):
        if count:
            print(f"{count}\t{rule_text}")


if __name__ == "__main__":
    main()
