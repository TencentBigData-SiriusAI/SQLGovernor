"""Shared prompt-rule builders for structured planner and renderer."""

from __future__ import annotations

import re
from typing import Iterable

from ..tools import FewShotExample
from .schema_graph import SchemaGraph

AGGREGATE_KEYWORDS = ["how many", "average", "ratio", "percentage", "percent"]
EXTREME_KEYWORDS = ["top", "highest", "lowest", "rank", "order", "largest", "smallest"]
ROUND_KEYWORDS = [
    "round",
    "rounded",
    "decimal",
    "decimals",
    "nearest",
    "precision",
    "significant",
]
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


def build_planner_rules(
    question: str,
    evidence: str,
    db_id: str | None,
    graph: SchemaGraph | None = None,
) -> str:
    combined = _combined_text(question, evidence)
    lines = [
        "- First identify the output entities/attributes, filter literals, aggregation target, and ordering requirement before proposing paths.",
        "- Prefer concise table-sets and shortest plausible join paths.",
        "- Explicitly choose owner tables for duplicated concepts instead of mixing semantically similar columns from different tables.",
        "- Do not introduce extra filters or business logic that the question/evidence does not state.",
        "- Flag risky paths that depend on broad slot-family expansion, correlated subqueries, or unnecessary bridge tables.",
        "- Prefer the minimum number of tables needed to express the question. Extra tables are a liability unless they contribute a required output, filter, aggregate, or join bridge.",
        "- If two candidate paths answer the same question, prefer the path with fewer joins, fewer detours, and fewer descriptor/helper tables.",
    ]
    if graph and graph.column_ambiguities:
        lines.append(
            "- Several concepts appear in multiple tables. Resolve ownership deliberately and keep the owner consistent through the whole plan."
        )
    lines.extend(_domain_rules(db_id))
    lines.extend(_dynamic_rules(combined, planner_mode=True))
    return "\n".join(lines)


def build_renderer_rules(
    question: str,
    evidence: str,
    db_id: str | None,
    graph: SchemaGraph | None = None,
) -> str:
    combined = _combined_text(question, evidence)
    lines = [
        "- Output exactly the information requested by the question; do not add or omit columns.",
        "- Never use SELECT *.",
        "- Follow the selected path, owner decisions, key-family choices, and slot strategy unless absolutely required to satisfy the question.",
        "- Do not introduce extra tables unless they are strictly necessary and justified by the schema.",
        "- Do not introduce extra filters beyond the question/evidence.",
        "- Return valid SQLite SQL only.",
        "- Prefer the shortest SQL that fully answers the question. Do not expand to semantically richer detours if a shorter SQL already satisfies the request.",
        "- If the selected path already exposes the needed output/filter/aggregate columns, do not replace it with a broader path.",
        "- Keep code-vs-description distinctions intact. If the question asks for a code/id, do not silently replace it with a type/name/description column, and vice versa.",
    ]
    if graph and graph.column_ambiguities:
        lines.append(
            "- When multiple columns seem semantically similar, honor the chosen owner decision instead of mixing fields from different tables."
        )
    lines.extend(_domain_rules(db_id))
    lines.extend(_dynamic_rules(combined, planner_mode=False))
    return "\n".join(lines)


def _combined_text(question: str, evidence: str) -> str:
    return f"{(question or '').strip().lower()} {(evidence or '').strip().lower()}".strip()


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


def _dynamic_rules(combined_text: str, *, planner_mode: bool) -> list[str]:
    lines: list[str] = []
    requires_aggregate = any(keyword in combined_text for keyword in AGGREGATE_KEYWORDS)
    requires_extreme = any(keyword in combined_text for keyword in EXTREME_KEYWORDS)
    decimal_requirement = _extract_decimal_places(combined_text)
    requires_rounding = any(keyword in combined_text for keyword in ROUND_KEYWORDS)

    if requires_aggregate:
        if planner_mode:
            lines.append(
                "- Because the question asks for counts/averages/ratios, include an explicit aggregate_spec and ensure candidate paths expose the needed measure column."
            )
        else:
            lines.append(
                "- Because the question asks for counts/averages/ratios, the SQL must contain the corresponding COUNT/AVG/SUM/ratio expression rather than only raw fields."
            )
    if requires_extreme:
        if planner_mode:
            lines.append(
                "- Because the question asks for an extremum/top result, include an ordering_spec and prefer paths that can support ORDER BY ... LIMIT cleanly."
            )
        else:
            lines.append(
                "- If the question asks for highest/lowest/top results, use ORDER BY ... LIMIT (or a ranking equivalent) rather than returning all rows."
            )
    if decimal_requirement is not None and requires_rounding:
        lines.append(
            f"- Match the requested numeric precision by using ROUND(..., {decimal_requirement}) in the final SQL."
        )
    if any(term in combined_text for term in ["percent", "percentage", "%", "ratio", "proportion"]):
        lines.append(
            "- Percentage or ratio calculations must use floating-point arithmetic, e.g. CAST(... AS REAL) or multiply by 1.0 before division."
        )
    return lines


def _domain_rules(db_id: str | None) -> list[str]:
    lowered = (db_id or "").strip().lower()
    rules = [
        "- If a concept appears in multiple tables, choose the table that truly owns the field according to schema comments and table semantics.",
    ]
    if lowered == "codebase_community":
        rules.extend(
            [
                "- For StackOverflow-style data, use users.DisplayName for user names and avoid relying on posts.OwnerDisplayName.",
                "- Tags stored in posts.Tags are angle-bracket strings; use tags metadata only when the question explicitly needs tag entities.",
                "- postHistory is a bridge/history table and should be used when the question asks about history events rather than current post rows.",
            ]
        )
    elif lowered == "formula_1":
        rules.extend(
            [
                "- In Formula 1 data, races.name is the Grand Prix name and circuits.name is the track name; do not confuse them.",
                "- Lap-specific metrics usually belong to lapTimes, while race summary metrics often belong to results.",
            ]
        )
    elif lowered == "thrombosis_prediction":
        rules.extend(
            [
                "- In thrombosis data, Patient stores demographics, Laboratory stores lab measurements, and Examination stores diagnoses/symptoms.",
                "- Do not source lab measurement columns from Patient or Examination.",
            ]
        )
    elif lowered == "california_schools":
        rules.extend(
            [
                "- In California schools data, duplicated school/district concepts may appear in schools and frpm; pick the owning table that matches the requested concept precisely.",
                "- Prefer frpm for meal-rate and FRPM-related metrics, and satscores for SAT-related metrics.",
            ]
        )
    elif lowered == "european_football_2":
        rules.extend(
            [
                "- In European football data, distinguish id from player_api_id/team_api_id/player_fifa_api_id; do not join across key families casually.",
                "- Treat Match home/away player columns as a slot family; avoid broad slot expansion unless the selected path explicitly requires it.",
            ]
        )
    return rules


def render_few_shot_section(examples: Iterable[FewShotExample]) -> str:
    examples = list(examples)
    if not examples:
        return "(No contextual examples available.)"

    blocks: list[str] = []
    for example in examples:
        question_text = example.question.strip() if example.question else "(missing question)"
        evidence_text = example.evidence.strip()
        sql_text = example.sql.strip() if example.sql else "-- missing --"

        lines = [f"-- Question:\n{question_text}"]
        if evidence_text:
            lines.append(f"-- Evidence:\n{evidence_text}")
        lines.extend(["```sql", sql_text, "```"])
        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)
