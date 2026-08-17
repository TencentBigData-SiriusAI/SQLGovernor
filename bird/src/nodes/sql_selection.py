"""Reward model assisted SQL candidate selection."""

from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import json
import os
from pathlib import Path
import random
import re
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
from loguru import logger
from src.tools.sql_result_voter import run_cached_voting, canonicalize_rows
from src.utils.schema_utils import get_related_schema
from transformers import AutoTokenizer
from openai import OpenAI

from config import Settings
from ..core.state import SQLAgentState
from ..tools.database import generate_mschema_str

SYSTEM_PROMPT = """# Role: SQL Evaluator
Your task is to determine which of two SQL responses (A and B) correctly answers the User Question based on the Schema.

# Evaluation Protocol (Strictly Follow):
1. Comparative Diff: First, identify the exact logic difference between SQL A and SQL B (e.g., extra WHERE clause, different JOIN, or ORDER BY).

2. Constraint Necessity (Crucial): If one SQL includes a filter/condition that the other omits, verify if the Question explicitly requests it.

- Rule: If a SQL adds a constraint (e.g., WHERE type = 'D') not mentioned in the Question, it is likely Incorrect (Over-constrained).

- Rule: The SQL that answers the question with the fewest assumptions is usually Correct.

3. Logic over Results: Judge solely based on code logic mapping to the question. NEVER conclude correctness because "the execution result looks right".

# Output Format:
1. Difference Analysis: [Briefly describe the difference]

2. Logic Check: [For each SQL, state if its logic (especially filters) is fully supported by the question text]

3. Final Judgment: <sql1_judge>Correct/Incorrect</sql1_judge> <sql2_judge>Correct/Incorrect</sql2_judge>

# Input:
Task Description: 
-- Database Schema: 
{schema}
-- Question: {question}
-- External Knowledge: {external_knowledge}

Responses: 
<START_OF_RESPONSE_1>
{sql1}
<END_OF_RESPONSE_1>
<START_OF_RESPONSE_2>
{sql2}
<END_OF_RESPONSE_2>
"""


SQL_TEMPLATE = """-- SQL: {sql}
-- Execution Result #rows: {num_rows}
-- Execution Result START
{result}
-- END Execution Result
"""


def sql_selection_node(state: SQLAgentState) -> Dict[str, Any]:
    """Blend reward-model scores and execution similarity to pick final SQL."""

    logger.debug("=" * 60)
    logger.debug("Starting SQL selection")
    logger.debug("=" * 60)

    generation_candidates = state.get("generation_candidates", [])
    execution_records: List[Optional[Dict[str, Any]]] = list(
        state.get("candidate_execution_results", [])
    )

    if not Settings.ENABLE_SQL_SELECTION_BLEND:
        logger.info(
            "SQL selection blend disabled; falling back to major voting."
        )
        fallback = _build_fallback_candidate(state)
        return {
            "final_sql": fallback,
            "scores_payload": [],
            "candidate_sql": fallback.get("sql", ""),
            "execution_result": fallback.get("exec_result", {}),
        }

    valid_candidates: List[Dict[str, Any]] = []
    for idx, candidate in enumerate(generation_candidates):
        exec_record: Optional[Dict[str, Any]] = None
        if idx < len(execution_records):
            exec_record = execution_records[idx]
        if not exec_record or not exec_record.get("success"):
            continue
        snapshot = deepcopy(candidate)
        snapshot.setdefault("index", idx)
        exec_record["rows"] = canonicalize_rows(exec_record.get("result", []))
        snapshot["exec_result"] = exec_record
        valid_candidates.append(snapshot)

    if not valid_candidates:
        logger.error("No valid executed candidates; falling back.")
        fallback = _build_fallback_candidate(state)
        return {
            "final_sql": fallback,
            "scores_payload": [],
            "candidate_sql": fallback.get("sql", ""),
            "execution_result": fallback.get("exec_result", {}),
        }

    try:
        logger.info(
            f"Running reward model question_id={state.get('question_id')} count={len(valid_candidates)}",
        )
        database_path = state.get("database_path", "") or os.path.join(Settings.DATABASE_DIR, state.get("db_id"), state.get("db_id") + ".sqlite")
        rm_scores, rm_scores_detail = _generate_rm_scores(
            candidates=valid_candidates,
            state=state,
            database_path=database_path,
            question_id=state.get("question_id"),
            batch_size=Settings.RM_BATCH_SIZE,
            max_workers=Settings.RM_MAX_WORKERS,
            max_retries=Settings.RM_MAX_RETRY_ATTEMPTS,
            retry_backoff=Settings.RM_RETRY_BACKOFF,
        )
        judge_details = rm_scores_detail.get("judge_details", []) if rm_scores_detail else []
        valid_count = sum(1 for item in judge_details if item.get("valid"))
        logger.debug(
            f"RM results question_id={state.get('question_id')} pairs={len(judge_details)} valid={valid_count}",
        )
        if len(judge_details) > 0 and valid_count == 0:
            sample_text = (judge_details[0].get("text") or "")[:200]
            logger.warning("RM returned invalid judgments: {}", sample_text)
    except Exception as exc:  # pragma: no cover - network failure
        rm_scores_detail = None
        logger.error(f"RM scoring failed: {exc}")
        rm_scores = [0.0] * len(valid_candidates)

    result_voting_summary = run_cached_voting(
        candidates=valid_candidates,
        job_results=execution_records,
    )
    maj_scores = result_voting_summary["maj_scores"]

    combined_score = _select_best_candidate(
        rm_scores=rm_scores,
        freq_scores=maj_scores,
        normalize_method=Settings.SQL_SELECTION_NORMALIZE,
        alpha=Settings.SQL_SELECTION_ALPHA,
    )
    for item, rm_score, freq_score, comb_score in zip(valid_candidates, rm_scores, maj_scores, combined_score):
        item["rm_score"] = rm_score
        item["freq_score"] = freq_score
        item["final_score"] = comb_score

    selected_idx = np.argmax(combined_score)
    selected_sql = valid_candidates[selected_idx]

    logger.info(
        f"Selected #{selected_sql.get('index', selected_idx)}, rm={rm_scores[selected_idx]:.4f}, maj={maj_scores[selected_idx]:.4f}"
    )

    final_payload = {
        "sql": selected_sql.get("sql", ""),
        "index": selected_sql.get("index", selected_idx),
        "exec_result": selected_sql.get("exec_result", {}),
        "rm_score": rm_scores[selected_idx],
        "freq_score": maj_scores[selected_idx],
        "final_score": combined_score[selected_idx]
    }
    scores_payload = [{
        "index": x["index"],
        "rm_score": x["rm_score"],
        "freq_score": x["freq_score"],
        "final_score": x["final_score"]
    } for x in valid_candidates]

    return {
        "final_sql": final_payload,
        "scores_payload": scores_payload,
        "candidate_sql": final_payload["sql"],
        "execution_result": final_payload.get("exec_result", {}),
        "rm_scores_detail": rm_scores_detail
    }


def _build_fallback_candidate(state: SQLAgentState) -> Dict[str, Any]:
    """Return the best available SQL when no executed candidate succeeded."""

    fallback_sql = state.get("candidate_sql") or ""
    fallback_exec = state.get("execution_result", {}) or {}
    fallback_index = state.get("current_candidate_index")

    attempted = state.get("attempted_candidates", [])
    if attempted:
        last_attempt = attempted[-1]
        fallback_sql = last_attempt.get("sql", fallback_sql)
        fallback_exec = last_attempt.get("execution") or fallback_exec
        fallback_index = last_attempt.get("queue_index", fallback_index)
    elif not fallback_sql and state.get("generation_candidates"):
        first_candidate = state["generation_candidates"][0]
        fallback_sql = first_candidate.get("sql", "")
        fallback_index = first_candidate.get("index", 0)

    return {
        "sql": fallback_sql or "",
        "index": fallback_index if fallback_index is not None else -1,
        "exec_result": fallback_exec or {},
        "rm_score": 0.0,
        "freq_score": 0.0,
    }


class RMHelper:
    """Prepare reward-model prompts from schema and execution results."""

    def __init__(
        self,
        *,
        database_path: Optional[str],
        tokenizer_name: str,
        max_results_tokens: int,
    ) -> None:
        if not database_path:
            raise RuntimeError("database_path is required")
        self.mschema = self._load_mschema(database_path)
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
        self.max_results_tokens = max_results_tokens

        # API
        self._check_api_health()

    def _load_mschema(self, database_path: str) -> str:
        schema_path = database_path.replace(".sqlite", ".xmschema")
        schema_file = Path(schema_path)
        if not schema_file.exists():
            logger.debug(f"Schema file missing, generating: {schema_file}")
            try:
                mschema_text = generate_mschema_str(database_path)
                schema_file.write_text(mschema_text, encoding="utf-8")
                logger.debug(f"Generated xmschema: {schema_file}")
                return mschema_text
            except Exception as exc:
                logger.warning(
                    f"Failed to generate schema {schema_file}: {exc}; trying JSON fallback.",
                )
            fallback_schema_path = (
                Path(Settings.DATA_DIR).parent
                / "data"
                / "schemas"
                / "dev_bird_schema_full.json"
            )
            if fallback_schema_path.exists():
                logger.debug(
                    f"Schema missing {schema_file}; using JSON fallback: {fallback_schema_path}",
                )
                return self._extract_schema_from_json(
                    str(fallback_schema_path), database_path
                )
            raise RuntimeError(
                f"Schema not found: {schema_file}"
            )
        
        table_schema = {}
        lines = []
        table_name = None
        
        with open(schema_path) as f:
            f.readline()
            f.readline()
            for line in f:
                if line.startswith("# Table: "):
                    if len(lines) > 0:
                        table_schema[table_name] = lines
                        lines = []
                    table_name = line.split(": ")[1].strip().lower()
                elif line.startswith("【Foreign keys】"):
                    if len(lines) > 0:
                        table_schema[table_name] = lines
                        lines = []
                    lines = [line.strip()]
                elif line.strip() not in ["[", "]"]:
                    lines.append(line.strip())
        with open(schema_path) as f:
            full = f.read()

        return {
            "tables": table_schema,
            "fks": "\n".join(lines),
            "full": full
        }

    def _extract_schema_from_json(self, json_path: str, database_path: str) -> str:
        """Extract the schema for a given db_id from a JSON schema cache."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                schema_data = json.load(f)

            # Derive db_id from the database path (e.g. /path/to/database.sqlite -> database)
            db_id = Path(database_path).stem

            # Look up the matching entry in the JSON schema cache
            for entry in schema_data:
                if entry.get("db_id") == db_id:
                    schema_text = entry.get("schema", "")
                    if schema_text:
                        logger.debug(f"Found schema for db_id={db_id}, length: {len(schema_text)}")
                        return schema_text

            # Fall back to the first entry if no exact match is found
            if schema_data and len(schema_data) > 0:
                first_schema = schema_data[0].get("schema", "")
                if first_schema:
                    logger.warning(f"No schema for db_id={db_id}; using first entry as fallback")
                    return first_schema

            raise RuntimeError(f"No schema found in JSON cache for db_id={db_id}")

        except Exception as e:
            raise RuntimeError(f"JSON schema: {e}")

    def _check_api_health(self) -> None:
        """Check that the reward-model endpoint is reachable."""
        try:
            client = OpenAI(
                base_url=Settings.RM_BASE_URL,
                api_key="",
            )
            logger.debug(
                f"API: {Settings.RM_BASE_URL}",
            )
            client.chat.completions.create(
                model=Settings.RM_MODEL_NAME,
                messages=[{"role": "user", "content": "health check"}],
                temperature=0.0,
                max_tokens=1,
            )
            logger.debug("API health check passed")
        except Exception as exc:
            logger.warning(
                f"API health check failed: {exc}",
            )

    def _compress_exec_result(self, results: Sequence[Sequence[Any]] | None) -> str:
        if not results:
            return "(empty set)"

        lines = [
            "\t".join(
                [
                    f"{value:.3f}" if isinstance(value, float) else str(value)
                    for value in row
                ]
            )
            for row in results
        ]
        raw = "\n".join(lines)
        encoded = self.tokenizer.encode(raw)
        if len(encoded) > self.max_results_tokens:
            truncated = self.tokenizer.decode(encoded[: self.max_results_tokens])
            return truncated + "...[TRUNCATED]"
        return raw

    def _get_deduped_sqls(self, candidates):
        
        group_id_list = []
        group_map = {}
        current_group_id = 0
        for i, candidate in enumerate(candidates):
            exec_result = candidate.get("exec_result", {}) or {}
            rows = exec_result.get("rows", {})
            x_normalized = rows
            
            if x_normalized not in group_map:
                # Assign a group id for each distinct execution result
                group_map[x_normalized] = current_group_id
                current_group_id += 1
            group_id_list.append(group_map[x_normalized])

        deduped_sql_ids = []
        score_grouping = {}
        for i in range(max(group_id_list) + 1):
            deduped_sql_ids.append(random.choice([j for j in range(len(group_id_list)) if group_id_list[j] == i]))
            for j in range(len(group_id_list)):
                if group_id_list[j] == i:
                    score_grouping[j] = deduped_sql_ids[-1]
        logger.debug(f"deduped sql ids: {deduped_sql_ids}, score_grouping: {score_grouping}")
        return deduped_sql_ids, score_grouping

    def make_prompts(
        self, state: SQLAgentState, candidates: Sequence[Dict[str, Any]]
    ) -> List[List[int, int, str]]:
        sql_ids, score_grouping = self._get_deduped_sqls(candidates)
        logger.info(
            "RM scoring question_id={} count={}",
            state.get("question_id"),
            len(sql_ids),
        )
        sqls = [candidates[i]["sql"] for i in sql_ids]
        mschema = get_related_schema(sqls, schema=self.mschema, logger=logger)
    
        def make_prompt(sql1_id, sql2_id):
            sql1_res = candidates[sql1_id]["exec_result"].get("result", [])
            sql2_res = candidates[sql2_id]["exec_result"].get("result", [])
            sql1_prompt = SQL_TEMPLATE.format(sql=candidates[sql1_id]["sql"], result=self._compress_exec_result(sql1_res), num_rows=len(sql1_res))
            sql2_prompt = SQL_TEMPLATE.format(sql=candidates[sql2_id]["sql"], result=self._compress_exec_result(sql2_res), num_rows=len(sql2_res))
            prompt1 = SYSTEM_PROMPT.format(schema=mschema, question=state.get("question", ""), external_knowledge=state.get("evidence", ""),sql1=sql1_prompt, sql2=sql2_prompt)
            prompt2 = SYSTEM_PROMPT.format(schema=mschema, question=state.get("question", ""), external_knowledge=state.get("evidence", ""),sql1=sql2_prompt, sql2=sql1_prompt)
            return [[sql1_id, sql2_id, prompt1], [sql2_id, sql1_id, prompt2]]

        prompts = []
        for i in range(len(sql_ids)):
            for j in range(i + 1, len(sql_ids)):
                prompts.extend(make_prompt(sql_ids[i], sql_ids[j]))
        return prompts, score_grouping


def _generate_rm_scores(
    *,
    candidates: Sequence[Dict[str, Any]],
    state: SQLAgentState,
    database_path: str,
    question_id: int,
    batch_size: int,
    max_workers: int,
    max_retries: int,
    retry_backoff: float,
) -> List[float]:
    """Call reward model in parallel batches to score candidates."""

    helper = RMHelper(
        database_path=database_path,
        tokenizer_name=Settings.RM_TOKENIZER,
        max_results_tokens=Settings.RM_MAX_RESULTS_TOKENS,
    )
    reward_prompts, score_grouping = list(helper.make_prompts(state, candidates))
    logger.debug(
        f"RM prompts question_id={question_id} count={len(reward_prompts)}",
    )

    if not reward_prompts:
        logger.warning(f"RM: no prompts question_id={question_id}")
        return [0.0] * len(candidates), {
            "judge_details": [],
            "scores": {i: 0.0 for i in range(len(candidates))},
        }

    try:
        client = OpenAI(
            base_url=Settings.RM_BASE_URL,
            api_key="",
        )
    except Exception as exc:
        logger.error(
            f"Failed to initialize SQL client question_id={question_id}: {exc}",
        )
        raise exc

    def worker(batch_prompts: Sequence[str], start_idx: int) -> List[Dict[str, Any]]:
        batch_scores = _call_reward_api(
            client=client,
            model_name=Settings.RM_MODEL_NAME,
            prompts=list(batch_prompts),
            question_id=question_id,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            timeout=Settings.REQUEST_TIMEOUT
        )
        return batch_scores

    actual_batch_size = max(1, batch_size)
    worker_count = max(1, max_workers)
    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = []
        for start in range(0, len(reward_prompts), actual_batch_size):
            slice_end = start + actual_batch_size
            futures.append(
                executor.submit(worker, reward_prompts[start:slice_end], start)
            )

        errors: List[Exception] = []
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(f"Worker failed question_id={question_id}: {exc}")
                errors.append(exc)

    if errors and not results:
        raise errors[0]

    results = sorted(results, key=lambda item: item.get("index", 0))
    score_details = _merge_rm_scores(results)
    rm_score = [score_details["scores"][score_grouping[i]] for i in range(len(candidates))]
    logger.debug(f"judge score: {score_details['scores']}, rm_score: {rm_score}")
    return rm_score, score_details


def _merge_rm_scores(judge_details):
    def elo_rating(ratings):
        scores = defaultdict(int)
        for rat in ratings:
            if not rat["valid"]:
                continue
            id1, id2 = rat["id1"], rat["id2"]
            i, j = rat["preds"]
            scores[id1] += i - j
            scores[id2] += j - i
        return scores

    scores = elo_rating(judge_details)
    
    return {
        "judge_details": judge_details,
        "scores": dict(scores),
    }


def normalize(score: Sequence[float], method: str) -> np.ndarray:
    data = np.array(score, dtype=float)
    if method == "exp":
        return np.exp(data)
    elif method == "sigmoid":
        return 1 / (1 + np.exp(-data))
    elif method == "sigmoid_mean":
        mean = data.mean() if len(data) else 0.0
        return 1 / (1 + np.exp(-(data - mean)))
    elif method == "minmax":
        min_val = data.min() if len(data) else 0.0
        max_val = data.max() if len(data) else 1.0
        denom = max(max_val - min_val, 1e-6)
        return (data - min_val) / denom
    elif method == "identity":
        return data
    else:
        # default softmax
        shifted = data - data.max(initial=0.0)
        exp_data = np.exp(shifted)
        denom = exp_data.sum() if exp_data.sum() else 1.0
        return exp_data / denom


def _select_best_candidate(
    *,
    rm_scores: Sequence[float],
    freq_scores: Sequence[float],
    normalize_method: str,
    alpha: float,
) -> int:
    rm_norm = normalize(rm_scores, normalize_method)
    freq_norm = normalize(freq_scores, normalize_method)
    combined = alpha * rm_norm + (1 - alpha) * freq_norm
    return combined


def extract_sql_judgment(text: str, sqls):
    preds = []
    for sql_idx in [1, 2]:
        pattern = r'<sql{sql_idx}_judge>\s*(Correct|Incorrect)\s*</sql{sql_idx}_judge>'.format(sql_idx=sql_idx)

        # re.IGNORECASE (re.I) matches C/c and I/i
        all_matches = re.findall(pattern, text, re.IGNORECASE)

        if all_matches:
            last_match = all_matches[-1]

            preds.append(last_match.lower() == "correct")
        else:
            preds.append(None)
    return {
        "id1": sqls[0],
        "id2": sqls[1],
        "valid": not None in preds,
        "preds": preds,
        # "text": text
    }


def _call_reward_api(
    client: OpenAI,
    model_name: str,
    *,
    prompts: List[str],
    question_id: Any,
    max_retries: int,
    retry_backoff: float,
    timeout: int,
) -> List[float]:
    """Call reward model HTTP endpoint with retry logic."""
    outputs = []
    last_error: Optional[Exception] = None
    for i in range(len(prompts)):
        for attempt in range(1, max_retries + 1):
            try:
                msg = [{"role": "user", "content": prompts[i][-1]}]
                response = client.chat.completions.create(
                    model=model_name,
                    messages=msg,
                    temperature=1.0,
                    max_tokens=512,
                    n=1,
                    timeout=timeout,
                )
                outputs.append(extract_sql_judgment(response.choices[0].message.content or "", prompts[i][:2]))
                break
            except Exception as exc:  # pragma: no cover - network failure
                last_error = exc
                wait_time = retry_backoff * attempt
                logger.warning(
                    "SQL API call failed (attempt {}/{}): {}, retrying in {:.1f}s",
                    attempt,
                    max_retries,
                    exc,
                    wait_time,
                )
                time.sleep(wait_time)
    if len(outputs) == len(prompts):
        return outputs

    # Classify the error to emit a helpful hint.
    error_type = type(last_error).__name__
    logger.error(f"RM API failed question_id={question_id}: {error_type} - {last_error}")

    # Provide actionable hints by error class.
    if "ConnectionError" in error_type or "ConnectTimeout" in error_type:
        logger.error("Hint: check the reward-model endpoint " + Settings.RM_BASE_URL)
        logger.error("Hint: 1) verify the endpoint URL 2) check network 3) confirm the model is served")
    elif "Timeout" in error_type:
        logger.error("Hint: increase the request timeout.")
    elif "HTTPError" in error_type:
        logger.error("Hint: HTTP error; check the API key and endpoint.")
    else:
        logger.error(f"Hint: unexpected error type {error_type}.")

    raise RuntimeError(f"RM scoring failed: {last_error}")
