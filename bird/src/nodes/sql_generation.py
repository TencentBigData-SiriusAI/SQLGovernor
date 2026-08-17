"""SQL generation node."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List
import re
import time

from loguru import logger
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage
from openai import OpenAI

from ..core.state import SQLAgentState
from ..tools import (
    FewShotExample,
    generate_corrected_sql,
    get_few_shot_examples,
)
from ..tools.sql_projection_rules import apply_projection_rules
from ..utils.llm_client import convert_messages_for_responses, extract_text_from_response
from ..utils.result_saver import save_generation_snapshot
from ..utils.sql_text import clean_sql_output
from ..tools.sql_validator import validate_sql_schema, validate_sql_syntax
from config import Settings
from config.model_config import get_model_config


DEFAULT_PROMPT_KEY = "agentar_primary"

SYSTEM_PROMPT_PRIMARY = (
    "You are a data science expert. Below, you are provided with a database schema "
    "and a natural language question. Your task is to understand the schema and "
    "generate a valid SQL query to answer the question."
)

SYSTEM_PROMPT_SECONDARY = (
    "You are a data science expert. Below, you are provided with a database schema "
    "and a natural language question. Your task is to understand the schema and "
    "generate a valid SQL query to answer the question."
)


# Contextual Examples:
# {few_shot_examples}
# Instructions:
# - Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
# - The generated query should return all of the information asked in the question without any missing or extra information.
# - Preserve natural ordering for related columns (e.g., Street, City, State, Zip or Phone, Ext, School) whenever those fields appear together.
# - Avoid projecting optional numbered variants (columns whose names end with 2, 3, etc.) unless the question explicitly asks for all such values.
# - If the question mentions multiple fields (e.g., both an ID and a name), ensure each of them appears in the SELECT clause—omitting any required columns will be marked incorrect.
# - Before generating the final SQL query, please think through the steps of how to write the query.

AGGREGATE_KEYWORDS = ["how many", "average", "ratio", "percentage", "percent"]
EXTREME_KEYWORDS = ["top", "highest", "lowest", "rank", "order"]
CALIFORNIA_SCHOOLS_DB_ID = "california_schools"
PERCENT_KEYWORDS = [
    "percent",
    "percentage",
    "%",
    "ratio",
    "proportion",
    "difference in percentage",
    "deviation",
]
ROUND_KEYWORDS = [
    "round",
    "rounded",
    "decimal",
    "decimals",
    "nearest",
    "precision",
    "significant",
]
DATE_TIME_KEYWORDS = [
    "year",
    "date",
    "month",
    "day",
    "between",
    "after",
    "before",
    "earliest",
    "latest",
    "oldest",
    "youngest",
    "age",
    "born",
]
NUMERIC_RANGE_KEYWORDS = [
    "normal range",
    "level",
    "range",
    "greater than",
    "less than",
    ">=",
    "<=",
    ">",
    "<",
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
# {dynamic_instructions}
# , and never use `SELECT *`

# Value Linking Hints (Soft Constraints):
# {value_link_hints}

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

# More detailed rules:
# 1. Skim the question and list every entity or attribute it references. For each, determine the source table by consulting the schema hints below. If an attribute exists in multiple tables (e.g., school names in `schools` vs `frpm`, diagnosis in `Patient` vs `Examination`), pick the table that truly owns the field according to the schema notes.
# 2. When the question mentions language, localized text, or foreign rulings, always use the `foreign_data` table (`language`, `text`). For multilingual card data, join `cards.uuid = foreign_data.uuid` and filter on `foreign_data.language`.
# 3. For StackOverflow data:
#    - Use `users.DisplayName` for user names, never rely on `posts.OwnerDisplayName`.
#    - Tags stored in `posts.Tags` are angle-bracket strings. If you need tag metadata, join `tags` via `tags.TagName` or `tags.ExcerptPostId`. Avoid `LIKE` unless absolutely necessary.
#    - Use `posts.ParentId` to distinguish questions (NULL) vs answers, and `PostTypeId=2` for answers.
# 4. For Formula 1 data:
#    - `races.name` contains Grand Prix names; `circuits.name` is the track name. When a question names a GP, filter on `races.name`.
#    - Lap-specific metrics (lap time, lap position) live in `lapTimes`, not `results`.
# 5. For thrombosis/medical data:
#    - Lab measurements (HGB, GOT, RF, ALB, etc.) come from `Laboratory`.
#    - Diagnoses and symptoms are stored in `Examination`. Only use `Patient` for demographics (sex, birthday).
# 6. When a question asks for “how many X” or “how many molecules/patients/cards”, default to `COUNT(DISTINCT entity_id)` unless the entity is inherently unique per row.
# 7. Percentages must use floating-point arithmetic: wrap the numerator with `CAST(... AS REAL)` or multiply by `1.0` before division.
# 8. Do not introduce extra filters that the question does not state. Return exactly the columns the question requests, in the same logical order (e.g., “name and id” → SELECT name, id).
# 9. Filter out NULL values before sorting or taking min/max unless the question explicitly wants NULLs.

# Value Linking Hints (Soft Constraints):
# {value_link_hints}
PROMPT_TEMPLATE_SECONDARY = """Database Engine:
SQLite

Database Schema:
{schema}
This schema describes the database’s structure, including tables, columns, primary keys, foreign keys, and any relevant relationships or constraints.

Question:
{question_with_knowledge}


Contextual Examples:
To help you, here are some examples of how natural language questions are translated into SQL queries. These examples illustrate common query patterns. Note that the schema for these examples may be different from the user's schema, so focus on the query structure and logic, not the specific table or column names.
{few_shot_examples}

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

PROMPT_VARIANTS = {
    "agentar_primary": {
        "system_prompt": SYSTEM_PROMPT_PRIMARY,
        "template": PROMPT_TEMPLATE_PRIMARY,
    },
    "secondary_fewshot": {
        "system_prompt": SYSTEM_PROMPT_SECONDARY,
        "template": PROMPT_TEMPLATE_SECONDARY,
    },
}


def _build_dynamic_instructions(question: str, evidence: str, db_id: str | None) -> str:
    """Create optional instruction bullets based on question/evidence cues."""

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
        (db_id or "").lower() == CALIFORNIA_SCHOOLS_DB_ID
        and "average score" in question_text
    )

    decimal_requirement = _extract_decimal_places(combined_text)
    round_keyword_hit = any(keyword in combined_text for keyword in ROUND_KEYWORDS)
    requires_rounding = bool(round_keyword_hit and decimal_requirement is not None)

    # The following advanced cues (percentage/date/range) are temporarily disabled.
    # requires_percentage = any(keyword in combined_text for keyword in PERCENT_KEYWORDS)
    # requires_date_handling = any(keyword in combined_text for keyword in DATE_TIME_KEYWORDS)
    # requires_numeric_cast = any(keyword in combined_text for keyword in NUMERIC_RANGE_KEYWORDS)
    # numeric_hints = _extract_numeric_hints(combined_text) if requires_numeric_cast else []

    extra_instr_lines: List[str] = []
    if requires_aggregate:
        extra_instr_lines.append(
            "- Because the question mentions counts/averages/ratios, ensure the SELECT clause "
            "contains the corresponding COUNT/AVG/SUM/ratio expression (do not omit it or "
            "replace it with raw fields)."
        )
    if requires_extreme:
        extra_instr_lines.append(
            "- If the question asks for highest/lowest/top results, use ORDER BY ... LIMIT (or "
            "RANK()) to pick the extremum rather than returning all rows."
        )
    if allows_avg_field:
        extra_instr_lines.append(
            "- For California schools, you may directly use AvgScrMath/AvgScrRead/AvgScrWrite "
            "columns when the question refers to average SAT scores."
        )

    # if requires_percentage:
    #     extra_instr_lines.append(
    #         "- The question explicitly expects a percentage; compute ratios with floating-point "
    #         "math and multiply by 100 with an informative alias."
    #     )
    if requires_rounding and decimal_requirement:
        extra_instr_lines.append(
            f"- Round the requested metric to {decimal_requirement} decimal places using ROUND(..., "
            f"{decimal_requirement}) so the output precision matches the question."
        )
    # if requires_date_handling:
    #     extra_instr_lines.append(
    #         "- Because the question compares years/dates, extract the year/month using "
    #         "STRFTIME/JULIANDAY (or substrings) before comparing rather than relying on raw "
    #         "string literals."
    #     )
    # if requires_numeric_cast:
    #     if numeric_hints:
    #         hint_text = ", ".join(f"'{hint}'" for hint in numeric_hints)
    #         extra_instr_lines.append(
    #             "- The question names thresholds " + hint_text + "; implement those comparisons "
    #             "with the exact operators and CAST text measurements to REAL before comparing."
    #         )
    #     else:
    #         extra_instr_lines.append(
    #             "- Respect the numeric thresholds/ranges literally (>, >=, BETWEEN). CAST text "
    #             "measurements to REAL when necessary so comparisons are numeric, not lexical."
    #         )

    if not extra_instr_lines:
        return ""

    return "\n" + "\n".join(extra_instr_lines)


def sql_generation_node(state: SQLAgentState) -> Dict[str, Any]:
    """Generate SQL candidates for the question."""

    logger.debug("=" * 60)
    logger.debug("Generating SQL")
    logger.debug("=" * 60)

    question = state["question"]
    evidence = state.get("evidence", "")
    schema_info = state.get("schema_info", {})
    question_id = state.get("question_id")

    schema_description = schema_info.get("schema_description", "")
    schema_text = schema_description.strip() or "(schema unavailable)"

    generation_attempt = state.get("generation_attempts", 0) + 1
    logger.debug(f"SQL generation attempt #{generation_attempt}")

    evidence_clean = evidence.strip() if isinstance(evidence, str) else ""
    question_clean = question.strip()
    raw_value_link_hints = state.get("value_link_prompt_hints")
    if isinstance(raw_value_link_hints, str) and raw_value_link_hints.strip():
        value_link_hints = raw_value_link_hints.strip()
    else:
        value_link_hints = "(No high-confidence value hints retrieved.)"

    if evidence_clean:
        question_with_knowledge = f"{evidence_clean}\n{question_clean}"
    else:
        question_with_knowledge = question_clean

    dynamic_instructions = _build_dynamic_instructions(
        question=question_clean,
        evidence=evidence_clean,
        db_id=state.get("db_id"),
    )

    generation_groups = _build_generation_groups(state.get("difficulty"))
    if not generation_groups:
        raise RuntimeError("SQL generation groups misconfigured (empty)")

    group_summary = ", ".join(
        f"{group['name']}[model={group['model_key']},count={group['candidate_count']},prompt={group['prompt_key']},temp={group['temperature']:.2f}]"
        for group in generation_groups
    )
    logger.debug(f"SQL generation groups: {group_summary}")

    prompt_payload_groups: List[Dict[str, Any]] = []
    message_history: List[BaseMessage] = list(state.get("messages", []))
    candidates: List[Dict[str, Any]] = []

    for group in generation_groups:
        variant = _resolve_prompt_variant(group["prompt_key"])
        system_prompt = variant["system_prompt"]
        prompt_template = variant["template"]

        few_shot_examples: List[FewShotExample] = []
        if group["few_shot_top_k"] > 0:
            few_shot_examples = get_few_shot_examples(
                question=question_clean,
                evidence=evidence_clean,
                top_k=group["few_shot_top_k"],
                min_score=group["few_shot_min_similarity"],
            )
            if few_shot_examples:
                logger.debug(
                    f"Few-shot examples group={group['name']} count={len(few_shot_examples)} top_k={group['few_shot_top_k']}",
                )
            else:
                logger.debug(
                    f"Group {group['name']} has no few-shot examples",
                )

        few_shot_block = _render_few_shot_section(few_shot_examples)
        generation_prompt = prompt_template.format(
            schema=schema_text,
            question_with_knowledge=question_with_knowledge,
            value_link_hints=value_link_hints,
            few_shot_examples=few_shot_block,
            dynamic_instructions=dynamic_instructions,
        )

        group_payload = {
            "group": group["name"],
            "model_key": group["model_key"],
            "prompt_key": group["prompt_key"],
            "schema": schema_text,
            "question": question,
            "external_knowledge": evidence_clean,
            "value_link_hints": value_link_hints,
            "few_shot_examples": [
                {
                    "question": example.question,
                    "evidence": example.evidence,
                    "sql": example.sql,
                    "db_id": example.db_id,
                }
                for example in few_shot_examples
            ],
            "user_prompt": generation_prompt,
            "system_prompt": system_prompt,
            "target_candidates": group["candidate_count"],
        }
        prompt_payload_groups.append(group_payload)

        message_history.extend(
            [SystemMessage(content=system_prompt), HumanMessage(content=generation_prompt)]
        )

        api_messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": generation_prompt},
        ]

        model_config = get_model_config(group["model_key"])
        try:
            client = OpenAI(
                base_url=model_config["base_url"],
                api_key=model_config["api_key"],
            )
        except Exception as exc:
            logger.error(
                f"Failed to init SQL client question_id={question_id} group={group['name']}: {exc}",
            )
            raise

        logger.debug(
            f"Generating {group['candidate_count']} candidates with model {model_config.get('model_name')} for group {group['name']} (temperature={group['temperature']:.2f})",
        )

        transport = model_config.get("transport", "chat_completions")
        effective_batch_size = model_config.get(
            "batch_size",
            1 if transport == "responses" else max(1, Settings.SQL_GENERATION_BATCH_SIZE),
        )
        max_batch_size = model_config.get("max_batch_size")
        if max_batch_size is not None:
            effective_batch_size = min(effective_batch_size, max(1, max_batch_size))

        try:
            group_candidates = _generate_candidates(
                client=client,
                model_name=model_config["model_name"],
                api_messages=api_messages,
                candidate_count=group["candidate_count"],
                batch_size=effective_batch_size,
                max_workers=max(1, Settings.SQL_GENERATION_VOTE_WORKERS),
                temperature=group["temperature"],
                max_tokens=Settings.SQL_GENERATION_MAX_TOKENS,
                request_timeout=Settings.SQL_GENERATION_REQUEST_TIMEOUT,
                max_retries=Settings.SQL_GENERATION_MAX_RETRIES,
                retry_backoff=Settings.SQL_GENERATION_RETRY_BACKOFF,
                schema_info=schema_info,
                database_path=state.get("database_path", ""),
                question_id=question_id,
                start_index=len(candidates),
                group_name=group["name"],
                source_model=model_config["model_name"],
                prompt_key=group["prompt_key"],
                question_text=question_clean,
                transport=transport,
            )
        except Exception as exc:
            logger.error(
                f"SQL generation failed question_id={question_id} group={group['name']}: {exc}"
            )
            raise

        candidates.extend(group_candidates)

        # Multiply the candidates if requested.
        multiply = group.get("multiply", 1)
        if multiply > 1:
            import copy
            original = list(group_candidates)
            for _ in range(multiply - 1):
                for cand in original:
                    dup = copy.deepcopy(cand)
                    dup["index"] = len(candidates)
                    dup["source"] = "generation_multiplied"
                    candidates.append(dup)

    if not candidates:
        logger.error(f"SQL generation produced no candidates question_id={question_id}")
        raise RuntimeError("SQL generation produced no candidates")

    logger.debug(
        f"Generated candidates question_id={question_id}, count={len(candidates)}",
    )

    sorted_queue = list(range(len(candidates)))

    snapshot_record = _build_generation_snapshot(
        question_id=question_id,
        db_id=state.get("db_id"),
        question=question_clean,
        candidates=candidates,
        voting_summary=None,
        auto_correct_applied=False,
        final_sql=candidates[0].get("sql", "") if candidates else "",
    )
    if snapshot_record:
        save_generation_snapshot(snapshot_record)

    return {
        "candidate_sql": "",
        "prompt_payload": {"groups": prompt_payload_groups},
        "generation_candidates": candidates,
        "candidate_queue": sorted_queue,
        "current_candidate_index": None,
        "current_candidate_meta": {},
        "current_attempt_index": None,
        "candidate_available": False,
        "generation_attempts": generation_attempt,
        "candidate_execution_results": [None] * len(candidates),
        "messages": message_history,
    }


def _resolve_prompt_variant(key: str) -> Dict[str, str]:
    variant = PROMPT_VARIANTS.get(key)
    if variant:
        return variant
    if key != DEFAULT_PROMPT_KEY:
        logger.warning("Unknown prompt key=%s, falling back to %s", key, DEFAULT_PROMPT_KEY)
    return PROMPT_VARIANTS[DEFAULT_PROMPT_KEY]


def _normalize_difficulty_bucket(raw_difficulty: Any) -> str:
    value = str(raw_difficulty or "").strip().lower()
    if value in {"simple", "easy"}:
        return "simple"
    if value in {"moderate", "medium"}:
        return "moderate"
    if value in {"challenging", "hard"}:
        return "challenging"
    return ""


def _parse_profile_string(profile_str: str) -> List[Dict[str, Any]]:
    """Parse a profile string like 'model:count:multiply:prompt:temperature;...' into group dicts."""
    groups: List[Dict[str, Any]] = []
    for i, segment in enumerate(profile_str.strip().split(";")):
        segment = segment.strip()
        if not segment:
            continue
        parts = segment.split(":")
        model_key = parts[0]
        count = int(parts[1]) if len(parts) > 1 else 16
        multiply = int(parts[2]) if len(parts) > 2 else 1
        prompt_key = parts[3] if len(parts) > 3 else "agentar_primary"
        temperature = float(parts[4]) if len(parts) > 4 else Settings.SQL_GENERATION_PRIMARY_TEMPERATURE

        # Hardcoded few-shot defaults by route
        if "secondary" in prompt_key:
            fsk = 5
            fs_min_sim = 0.0
        else:
            fsk = 0
            fs_min_sim = 0.0

        groups.append({
            "name": f"profile_{model_key}_{i}",
            "model_key": model_key,
            "prompt_key": prompt_key,
            "candidate_count": count,
            "temperature": temperature,
            "multiply": multiply,
            "few_shot_top_k": fsk,
            "few_shot_min_similarity": fs_min_sim,
        })
    return groups


def _build_generation_groups(raw_difficulty: Any) -> List[Dict[str, Any]]:
    """Build generation groups from profile configuration (sole entry point)."""
    bucket = _normalize_difficulty_bucket(raw_difficulty)
    profile_str = ""
    if bucket == "simple":
        profile_str = Settings.SQL_GEN_PROFILE_SIMPLE
    elif bucket == "moderate":
        profile_str = Settings.SQL_GEN_PROFILE_MODERATE
    elif bucket == "challenging":
        profile_str = Settings.SQL_GEN_PROFILE_CHALLENGING

    if not profile_str.strip():
        logger.error("No SQL_GEN_PROFILE configured for bucket=%s", bucket or "(empty)")
        return []

    groups = _parse_profile_string(profile_str)
    if groups:
        logger.debug("Using generation profile for %s: %s", bucket, profile_str)
    return groups


def _render_few_shot_section(examples: List[FewShotExample]) -> str:
    """Format few-shot examples for the prompt body."""

    if not examples:
        return "(No contextual examples available.)"

    blocks: List[str] = []
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


def build_reasoning_context(state: SQLAgentState) -> str:
    """Return empty reasoning context (pre-analysis disabled)."""

    return ""





def _generate_candidates(
    client: OpenAI,
    model_name: str,
    api_messages: List[Dict[str, Any]],
    candidate_count: int,
    batch_size: int,
    max_workers: int,
    temperature: float,
    max_tokens: int,
    request_timeout: int,
    max_retries: int,
    retry_backoff: float,
    schema_info: Dict[str, Any],
    database_path: str,
    question_id: Any,
    start_index: int,
    group_name: str,
    source_model: str,
    prompt_key: str,
    question_text: str,
    transport: str = "chat_completions",
) -> List[Dict[str, Any]]:
    """Call the LLM to generate SQL candidates."""

    requested_batch_size = max(1, batch_size)
    results: List[Dict[str, Any]] = []

    def worker(batch_index: int, current_batch_size: int) -> List[Dict[str, Any]]:
        responses = _call_sql_generation_api(
            client=client,
            model_name=model_name,
            messages=api_messages,
            temperature=temperature,
            max_tokens=max_tokens,
            n=current_batch_size,
            timeout=request_timeout,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            question_id=question_id,
            transport=transport,
        )

        batch_results: List[Dict[str, Any]] = []
        for offset, raw_text in enumerate(responses):
            index = start_index + batch_index * requested_batch_size + offset
            sql_text = clean_sql_output(raw_text or "")
            projection_adjustments: List[str] = []
            if Settings.SQL_PROJECTION_RULES_ENABLED and sql_text:
                adjusted_sql, adjustments = apply_projection_rules(sql_text, question_text)
                if adjustments:
                    sql_text = adjusted_sql
                    projection_adjustments = [item.description for item in adjustments]
            syntax_errors = validate_sql_syntax(sql_text) if sql_text else ["EMPTY_SQL"]
            schema_errors: List[str] = []
            if not syntax_errors:
                schema_errors = validate_sql_schema(sql_text, schema_info, database_path)
            errors = syntax_errors + schema_errors
            batch_results.append(
                {
                    "index": index,
                    "raw_response": raw_text,
                    "sql": sql_text,
                    "errors": errors,
                    "is_valid": not errors,
                    "exception": None,
                    "source": "generation",
                    "source_group": group_name,
                    "source_model": source_model,
                    "prompt_key": prompt_key,
                    "projection_adjustments": projection_adjustments,
                }
            )
        return batch_results

    worker_count = max(1, max_workers)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = []
        remaining = candidate_count
        batch_idx = 0
        while remaining > 0:
            current_batch = min(requested_batch_size, remaining)
            futures.append(executor.submit(worker, batch_idx, current_batch))
            remaining -= current_batch
            batch_idx += 1

        errors: List[Exception] = []
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as exc:  # pragma: no cover - worker failure
                logger.error(f"Worker failed question_id={question_id}: {exc}")
                errors.append(exc)

    if errors and not results:
        raise errors[0]

    vote_counter = Counter(
        candidate["sql"] for candidate in results if candidate.get("sql")
    )
    for candidate in results:
        candidate["vote_count"] = vote_counter.get(candidate.get("sql"), 0)

    return sorted(results, key=lambda item: item.get("index", 0))


def _build_generation_snapshot(
    question_id: Any,
    db_id: str | None,
    question: str,
    candidates: List[Dict[str, Any]],
    voting_summary: Dict[str, Any] | None,
    auto_correct_applied: bool,
    final_sql: str,
) -> Dict[str, Any] | None:
    """Assemble structured record for persistence."""

    if not Settings.SQL_GENERATION_SAVE_RESULTS:
        return None

    summary = voting_summary or {}
    success_count = sum(1 for item in candidates if item.get("execution_success"))
    failure_count = sum(1 for item in candidates if item.get("execution_success") is False)

    execution_summary = {
        "parallel_workers": Settings.SQL_RESULT_VOTING_WORKERS,
        "timeout_seconds": Settings.SQL_RESULT_VOTING_TIMEOUT,
        "total_candidates": len(candidates),
        "execution_successful": success_count,
        "execution_failed": failure_count,
        "winner_index": summary.get("winner_local_index"),
        "winner_score": summary.get("winning_score"),
        "auto_correct_applied": auto_correct_applied,
    }

    candidate_entries = []
    for candidate in candidates:
        triples = candidate.get("execution_triples") or []
        candidate_entries.append(
            {
                "index": candidate.get("index"),
                "sql": candidate.get("sql", ""),
                "vote_count": candidate.get("vote_count", 0),
                "result_vote_score": candidate.get("result_vote_score", 0.0),
                "is_valid": candidate.get("is_valid"),
                "errors": candidate.get("errors", []),
                "execution_success": candidate.get("execution_success"),
                "execution_error": candidate.get("execution_error"),
                "execution_row_count": candidate.get("execution_row_count"),
                "execution_triples": [list(triple) for triple in triples],
                "source": candidate.get("source"),
                "source_group": candidate.get("source_group"),
                "source_model": candidate.get("source_model"),
                "prompt_key": candidate.get("prompt_key"),
                "auto_correct_attempts": candidate.get("auto_correct_attempts", 0),
                "auto_correct_error": candidate.get("auto_correct_error"),
            }
        )

    return {
        "question_id": question_id,
        "db_id": db_id,
        "question": question,
        "final_sql": final_sql,
        "execution_summary": execution_summary,
        "sql_candidates": candidate_entries,
    }


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
    transport: str = "chat_completions",
) -> List[str]:
    """Call the SQL generation API with retry logic."""

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            if transport == "responses":
                request_count = max(1, n)
                outputs: List[str] = []
                payload = convert_messages_for_responses(messages)
                for _ in range(request_count):
                    response = client.responses.create(
                        model=model_name,
                        input=payload,
                        temperature=temperature,
                        max_output_tokens=max_tokens,
                        timeout=timeout,
                    )
                    outputs.append(extract_text_from_response(response))
                return outputs

            response = client.chat.completions.create(
                model=model_name,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                n=n,
                timeout=timeout,
            )
            return [choice.message.content or "" for choice in response.choices]
        except Exception as exc:  # pragma: no cover - network failure
            last_error = exc
            wait_time = retry_backoff * attempt
            if attempt < max_retries:
                logger.warning(
                    "SQL generation API call failed (attempt {}/{}): {}, retrying in {:.1f}s",
                    attempt,
                    max_retries,
                    exc,
                    wait_time,
                )
                time.sleep(wait_time)
            else:
                logger.warning(
                    "SQL generation API call failed (attempt {}/{}): {}",
                    attempt,
                    max_retries,
                    exc,
                )

    logger.error("SQL generation API failed question_id={}: {}", question_id, last_error)
    raise RuntimeError(f"SQL generation API failed: {last_error}")
