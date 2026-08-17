from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import itertools
import json
import random
import re
import tqdm
import os

import pandas as pd
from transformers import AutoTokenizer, AutoTokenizer
from tqdm import tqdm
from functools import partial
import numpy as np
from dotenv import load_dotenv

# Load .env (override existing env vars).
load_dotenv(override=True)

try:
    from .major_voting import _execute_sql_job, run_cached_voting, compare_sql
    from .utils import call_gpt
except ImportError:
    from major_voting import _execute_sql_job, run_cached_voting, compare_sql
    from utils import call_gpt


os.environ["TOKENIZERS_PARALLELISM"] = "false"
schema_backend = None


PROMPT = """# Role: SQL Evaluator
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


SCHEMA_PARSER_MODULES = {
    "sql_metadata": "utils",
    "sqlglot": "utils_sqlglot",
}


def get_schema_backend_module(parser_name):
    backend = parser_name.strip().lower()
    if backend not in SCHEMA_PARSER_MODULES:
        raise ValueError(f"Unsupported schema parser backend: {parser_name}")

    module_name = SCHEMA_PARSER_MODULES[backend]
    if __package__:
        return importlib.import_module(f"{__package__}.{module_name}")
    return importlib.import_module(module_name)


schema_backend = get_schema_backend_module("sql_metadata")


def process_results(results, max_results_tokens=256):
    encoded = tokenizer(
        results,
        add_special_tokens=False,
        return_attention_mask=False,
        return_token_type_ids=False,
        verbose=False,
    )["input_ids"]
    if len(encoded) > max_results_tokens:
        return tokenizer.decode(encoded[:max_results_tokens]) + "...[TRUNCATED]"
    else:
        return results


def get_effective_prompt_token_limit() -> int:
    configured = int(getattr(args, "max_prompt_tokens", 32768))
    tokenizer_limit = getattr(tokenizer, "model_max_length", None)
    if isinstance(tokenizer_limit, int) and 0 < tokenizer_limit < 1_000_000:
        return min(configured, tokenizer_limit)
    return configured


def prompt_exceeds_limit(prompt: str) -> tuple[bool, int, int]:
    token_count = len(
        tokenizer(
            prompt,
            add_special_tokens=False,
            return_attention_mask=False,
            return_token_type_ids=False,
            verbose=False,
        )["input_ids"]
    )
    limit = get_effective_prompt_token_limit()
    return token_count > limit, token_count, limit


def get_tsv_from_list_of_list(data):
    return "\n".join(
        [
            "\t".join([str(x) if not isinstance(x, float) else f"{x:.3f}" for x in row])
            for row in data
        ]
    )

def generate_sql_ele(res):
    return ("-- SQL: {}\n".format(res[0])
            + "-- Execution Result #rows: {}\n".format(res[1])
            + "-- Execution Result START\n{}\n".format(
                process_results(get_tsv_from_list_of_list(res[2]), max_results_tokens=256)
            )
            + "-- END Execution Result"
            )


def make_prompt(row, sql1_res, sql2_res, schema=None):
    sql1_prompt = generate_sql_ele(sql1_res)
    sql2_prompt = generate_sql_ele(sql2_res)
    prompt1 = PROMPT.format(schema=schema, question=row["question"], external_knowledge=row["external_knowledge"], 
                           sql1=sql1_prompt, 
                           sql2=sql2_prompt)
    prompt2 = PROMPT.format(schema=schema, question=row["question"], external_knowledge=row["external_knowledge"], 
                           sql1=sql2_prompt, 
                           sql2=sql1_prompt)
    return prompt1, prompt2


def extract_sql_judgment(text: str, sqls):
    preds = []
    for sql_idx in [1, 2]:
        pattern = r'<sql{sql_idx}_judge>\s*(Correct|Incorrect)\s*</sql{sql_idx}_judge>'.format(sql_idx=sql_idx)

        # re.IGNORECASE (re.I)  C/c, I/i
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
        "text": text
    }


def get_deduped_sqls(row):
    output = []
    group_ids = row["group_id_list"]
    for i in range(max(group_ids) + 1):
        output.append(random.choice([j for j in range(len(group_ids)) if group_ids[j] == i]))
    return output


def make_single_compare(sqls, row, db_path, schema):
    sql1, sql2 = row["pred_sqls"][sqls[0]], row["pred_sqls"][sqls[1]]
    label = compare_sql(
        db_path,
        row["question"],
        sql1,
        sql2,
        max_result_chars=int(getattr(args, "max_result_chars_for_prompt", 8000)),
    )
    sql1_result = [sql1, len(label["gold_result"]), label["gold_result"]]
    sql2_result = [sql2, len(label["pred_result"]), label["pred_result"]]
    
    prompts = make_prompt(row, sql1_result, sql2_result, schema)

    too_long_1, tokens_1, limit = prompt_exceeds_limit(prompts[0])
    too_long_2, tokens_2, _ = prompt_exceeds_limit(prompts[1])
    if too_long_1 or too_long_2:
        reason = (
            f"SKIPPED_PROMPT_TOO_LONG(limit={limit}, prompt1_tokens={tokens_1}, prompt2_tokens={tokens_2})"
        )
        invalid1 = {
            "id1": sqls[0],
            "id2": sqls[1],
            "valid": False,
            "preds": [None, None],
            "text": reason,
        }
        invalid2 = {
            "id1": sqls[1],
            "id2": sqls[0],
            "valid": False,
            "preds": [None, None],
            "text": reason,
        }
        return invalid1, invalid2

    llm_judge1 = extract_sql_judgment(llm_handler(prompts[0]), sqls=sqls)
    llm_judge2 = extract_sql_judgment(llm_handler(prompts[1]), sqls=sqls[::-1])

    return llm_judge1, llm_judge2


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


def flatten(l):
    return [item for sublist in l for item in sublist]


def flatten_scores(scores, group_ids):
    group_scores = {group_ids[int(k)]: v for k, v in scores.items()}
    output = []
    try:
        for i in range(len(group_ids)):
            output.append(group_scores[group_ids[i]] if group_ids[i] in group_scores else 0)
    except Exception as e:
        print(scores, group_ids, e)
    return output


def to_serializable(obj):
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_serializable(x) for x in obj]
    if isinstance(obj, tuple):
        return [to_serializable(x) for x in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    return obj


def append_checkpoint(path, record):
    serialized = to_serializable(record)
    with open(path, "a") as f:
        f.write(json.dumps(serialized, ensure_ascii=False) + "\n")


def load_processed_question_ids(path):
    if not os.path.exists(path):
        return set()

    processed = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            qid = rec.get("question_id") if isinstance(rec, dict) else None
            if qid is not None:
                processed.add(qid)
    return processed


def load_checkpoint_records(path):
    if not os.path.exists(path):
        return []

    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(rec, dict):
                records.append(rec)
    return records


def dump_outputs_from_checkpoint(file, checkpoint_path, output_suffix=""):
    records = load_checkpoint_records(checkpoint_path)
    if not records:
        print(f"no valid checkpoint records found in {checkpoint_path}")
        return

    df_out = pd.DataFrame(records)
    selection_path = f"{file}{output_suffix}_selection.json"
    final_path = f"{file}{output_suffix}_final.json"
    df_out.to_json(selection_path, orient="records", lines=True)

    if "pred_sqls" not in df_out.columns or "merged_idx" not in df_out.columns:
        print(f"selection results saved to {selection_path}")
        print("missing pred_sqls/merged_idx, skip final json generation")
        return

    df_out["final_sql"] = df_out.apply(lambda x: x["pred_sqls"][x["merged_idx"]], axis=1)
    df_out[["question_id", "final_sql"]].to_json(final_path, orient="records")
    print(f"final results saved to {final_path}")


def run_rm(row):
    db_id = row["db_id"]
    db_path = get_db_path(db_id)
    sql_ids = get_deduped_sqls(row)
    sqls = [row["pred_sqls"][i] for i in sql_ids]

    pairs = list(itertools.combinations(sql_ids, 2))
    mschema = schema_backend.get_related_schema(db_id, sqls)
    
    with ThreadPoolExecutor(max_workers=32) as executor:
        results = list(executor.map(partial(make_single_compare, db_path=db_path, row=row, schema=mschema), pairs))
 
    results = flatten(results)
    judge_details = results
    scores = elo_rating(results)

    if len(scores) > 0:
        final_idx = max(scores, key=scores.get)
    else:
        final_idx = random.choice(sql_ids)

    rm_score_flatten = flatten_scores(scores, row["group_id_list"])

    return {
        "question_id": row["question_id"],
        "rm_scores": rm_score_flatten,
        "pair_judge": {
        "judge_details": judge_details,
        "dedup_sql_ids": sql_ids,
        "scores": dict(scores),
        "pair_idx": final_idx,
    }}


def is_correct(pred, gt):
    if pred is None and gt is None:
        return True 
    elif pred is None or gt is None:
        return False 
    else:
        return frozenset(tuple(x) for x in pred) == frozenset(tuple(x) for x in gt)


def get_db_path(db_id):
    DATABASE_DIR = os.getenv("DATABASE_DIR", "data/dev_20240627/dev_databases")
    return f"{DATABASE_DIR}/{db_id}/{db_id}.sqlite"


def get_results(sql_with_index, db, timeout=30):
    index, sql = sql_with_index
    db_path = get_db_path(db)
    job = (db_path, db, sql, timeout)
    res = _execute_sql_job(job)
    return res


def get_frequency_and_group_id(triple_list, result_list, valid_list):
    def major_voting(triples, valid):
        all_sets = triples
        freq_idx = 0
        if sum(valid) == 0:
            freq_idx = random.choice(range(len(all_sets)))
            return freq_idx, None 

        triple_counts = {}
        for res, val in zip(all_sets, valid):
            if val:
                for triple in res:
                    if triple in triple_counts:
                        triple_counts[triple] += 1
                    else:
                        triple_counts[triple] = 1

        result_scores = []
        for i, (res, val) in enumerate(zip(all_sets, valid)):
            if val == 1:
                score = sum(triple_counts[triple] for triple in res if triple[2] is not None) / len(res) if res else 0
                result_scores.append(score)
            else:
                result_scores.append(0)

        if result_scores:
            freq_idx = np.argmax(result_scores)
        else:
            return 0, None
        return freq_idx, result_scores
    
    normalized_list = [tuple(x) if x is not None else None for x in result_list]
    
    counter = Counter(normalized_list)
    total_length = len(result_list)
    
    group_map = {}
    current_group_id = 0
    group_id_list = []
    freq_score = []

    for x_normalized in normalized_list:
        if x_normalized is None:
            freq_score.append(0)
            group_id_list.append(-1) 
        else:
            score = counter[x_normalized] / total_length
            freq_score.append(score)
            
            if x_normalized not in group_map:
                group_map[x_normalized] = current_group_id
                current_group_id += 1
            
            group_id_list.append(group_map[x_normalized])

    maj_idx, maj_score = major_voting(triple_list, valid_list)
            
    return maj_idx, maj_score, freq_score, group_id_list


def get_maj_results(examples):
    db = examples["db_id"]
    sqls = [candidate["sql"] for candidate in examples["sql_candidates"]]

    with ThreadPoolExecutor(max_workers=args.max_thread) as pool:
        results = list(pool.map(partial(get_results, db=db, timeout=1000), enumerate(sqls)))

    candidates = [{"sql": sql, "index": i, "errors": error} for i, (db, sql, rows, error) in enumerate(results)]
    weights = [1] * len(sqls)
    job_results = {(db, x[1]): (x[2], x[3]) for x in results}
    result_list = [x[2] for x in results]

    voting_results = run_cached_voting(candidates, job_results, db, voting_weights=weights)
    triple_list = [x["triples"] for x in voting_results["records"]]
    valid_list = [x["success"] for x in voting_results["records"]]
    maj_idx, maj_score, freq_score, group_id_list = get_frequency_and_group_id(triple_list, result_list, valid_list)

    rm_sample = {
        "db_id": db,
        "question_id": examples["question_id"],
        "pred_sqls": sqls,
        "group_id_list": group_id_list,
        "question": examples["question"],
        "external_knowledge": examples["evidence"],
    }
    difficulty = str(examples.get("difficulty", "")).strip().lower()
    skip_rm = args.skip_rm_for_simple and difficulty == "simple"
    if skip_rm:
        rm_score = [0.0] * len(maj_score)
    else:
        rm_result = run_rm(rm_sample)
        rm_score = rm_result["rm_scores"]

    merged_score = np.array(rm_score) + np.array(maj_score)

    return {
        "question_id": examples["question_id"],
        "maj_score_refine": voting_results["maj_scores"],
        "freq_score": freq_score,
        "valid_list": valid_list,
        "group_id_list": group_id_list,
        "maj_score": maj_score,
        "maj_idx": maj_idx,
        "rm_score": rm_score,
        "rm_skipped": skip_rm,
        "merged_score": merged_score,
        "merged_idx": np.argmax(merged_score),
        "pred_sqls": sqls,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, default="")
    parser.add_argument("--tokenizer", type=str, default="your-org/sql-selection-32b")
    parser.add_argument("--exp", type=str, default="")
    parser.add_argument("--max-token", type=int, default=512)
    parser.add_argument("--max-prompt-tokens", type=int, default=32768)
    parser.add_argument("--max-result-chars-for-prompt", type=int, default=8000)
    parser.add_argument("--max-thread", type=int, default=32)
    parser.add_argument("--checkpoint-path", type=str, default="")
    parser.add_argument("--output-suffix", type=str, default="")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-rm-for-simple", action="store_true")
    parser.add_argument(
        "--schema-parser",
        type=str,
        default="sql_metadata",
        choices=sorted(SCHEMA_PARSER_MODULES.keys()),
    )
    args = parser.parse_args()

    schema_backend = get_schema_backend_module(args.schema_parser)
    print(f"schema parser backend: {args.schema_parser}")

    tokenizer = AutoTokenizer.from_pretrained(os.getenv("RM_TOKENIZER", ""))

    llm_handler = partial(call_gpt, model=os.getenv("RM_MODEL_NAME", ""), 
                          openai_api_base=os.getenv("RM_BASE_URL", ""), 
                          max_tokens=args.max_token, temperature=0)

    file = args.file
    with open(file) as f:
        data = json.load(f)["results"]

    checkpoint_path = args.checkpoint_path or f"{file}{args.output_suffix}_selection.checkpoint.jsonl"
    if args.resume:
        processed_question_ids = load_processed_question_ids(checkpoint_path)
        print(f"resume mode: found {len(processed_question_ids)} finished samples in checkpoint")
    else:
        processed_question_ids = set()
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            print(f"removed stale checkpoint: {checkpoint_path}")

    pending_data = [d for d in data if d.get("question_id") not in processed_question_ids]
    print(f"total samples: {len(data)}, pending: {len(pending_data)}")

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = []
        for d in pending_data:
            futures.append(executor.submit(get_maj_results, d))

        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                out = future.result()
                append_checkpoint(checkpoint_path, out)
            except Exception as e:
                print(e)
                continue

    dump_outputs_from_checkpoint(file, checkpoint_path, output_suffix=args.output_suffix)
