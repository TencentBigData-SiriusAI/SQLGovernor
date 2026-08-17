# SQLGovernor — BIRD Dev Evaluation

This repository is the companion artifact for the BIRD dev-set experiments reported in the
SQLGovernor paper. It reproduces the two public-benchmark results:

| Paper section | Setting | Metric | Reported |
| --- | --- | --- | --- |
| Sec. 8.3.3 — SQL generation | single-pass and candidate-selection | Execution Accuracy (EX) | 73.1% |
| Sec. 8.6.2 — full agentic workflow | generation → correction → diagnosis → refinement → voting | Execution Accuracy (EX) | 73.9% |

## Reproducibility protocol

Consistent with the paper, the BIRD **dev split is offline-only**:

- Gold SQL and gold execution results are **not exposed** during generation, voting, feedback,
  or refinement.
- Refinement uses only non-label workflow signals (syntax/runtime errors, schema-checking
  feedback, and diagnostic messages).
- The comparison is a public dev-set check, not a hidden-test or leaderboard claim.

The paper's generation model is the **domain-adapted text-to-SQL model** trained with the
Task-Aligned Supervision Pipeline followed by SFT and RL (DAPO). 

## Pipeline overview

This artifact is a **BIRD-specific instantiation** of the paper's SQL Code Development and SQL
Code Debugging methodology. It exposes the benchmark path used for the public dev-set results;
it does not reproduce the private-domain knowledge base or the complete enterprise diagnostic
catalog described in the paper.

| BIRD stage | Code stage |
| --- | --- |
| Multi-candidate generation with the Path-II model | Phase 1 — public BIRD schema/evidence + `scripts/sql_pipeline_v2.py` |
| Executability verification | Phase 2 — parsing, precompilation, rule cleanup, and hard filtering |
| Execution-feedback diagnosis | Phase 3 — runtime-error, timeout, and empty-result signals |
| Execution-feedback-aware repair | Phase 4 — correction + re-execution |
| Structural/rule-guided refinement | Phase 5 — rule filtering + zero-candidate regeneration |
| Candidate selection | execution-result agreement + pairwise reward model + selection gates |

```text
Phase 1 generation -> Phase 2 validation -> Phase 3 execution feedback
-> Phase 4 repair -> Phase 5 refinement -> candidate selection -> EX evaluation
```

## Alignment with the paper's methodology

**Shared session context (Sec. 3).** Phase 1 uses `SQLAgentState` (`src/core/state.py`) to carry
question, schema, evidence, and generation artifacts. The later benchmark phases persist
validation diagnostics, execution outcomes, and correction history in the pipeline JSON;
selection scores and decisions are recorded in the selection checkpoint and final output files.
Together, these files provide the BIRD-specific realization of the paper's shared-context and
session-artifact abstractions.

**Dual-path scope (Sec. 4).** The default BIRD setting exercises the Path-II generation model:
the `Task-Aligned Supervision Pipeline` is applied to BIRD training data, followed by SFT and RL
with DAPO. It uses the public BIRD schema and evidence fields. Private-KB RAG is not part of this
public-benchmark configuration; optional few-shot retrieval exists in the code but is disabled
by the default profiles. Completion and post-event buffered promotion are outside this frozen
offline evaluation.

**Correctness verification (Sec. 3).** The BIRD path exercises executability checks through
parsing and precompilation. Semantic and promotion-acceptance checks belong to the private
repository/deployment setting and are not part of this dev-set EX protocol.

**Debugging scope (Sec. 6).** For BIRD, the paper's logic/efficiency-diagnosis and refinement
stages are instantiated with benchmark-observable, non-label signals: schema-checking feedback,
runtime errors, timeouts, empty results, and structural/rule-based diagnostics. Affected SQL is
repaired or regenerated and then re-executed. The private-domain diagnostic knowledge base and
its full enterprise issue catalog are outside this public-benchmark configuration.

## Model roles

The pipeline uses three roles, all served behind OpenAI-compatible endpoints:

- **Generation model** — the domain-adapted text-to-SQL model produced by Path II
  (`Task-Aligned Supervision Pipeline` + SFT + RL), referenced as `SQLGOVERNOR-GEN`
  (`SQLGOVERNOR-GEN-V2` for moderate/challenging questions).
- **Auxiliary reasoning model** — `qwen3-235b`, used by the repair, diagnosis, and
  timeout-specialist stages (the debugging/refinement support).
- **Reward (selection) model** — `SQL-SELECTION-MODEL`, used pairwise alongside
  execution-result agreement; the combined score is followed by structural and timeout gates.

The model names above are placeholders; set the matching endpoint, model name, and API key in
`.env`. The repository ships `.env.example` only — no real API keys or private endpoints.

**Model weights are not distributed.** The generation model and the reward (selection) model are
trained in-house (Sec. 4.2, Sec. 5.1) and their weights are not published in this repository. To
run the pipeline you must supply your own models behind OpenAI-compatible endpoints — either the
models trained as described in the paper, or any OpenAI-compatible text-to-SQL / reward models as
drop-in substitutes. With substitutes the pipeline runs end-to-end, but the resulting EX will
differ from the paper's reported 73.1% / 73.9%, which were obtained with the paper's own models.

## BIRD dev dataset

The BIRD dev split is not bundled here. Obtain the official dev release (the
[DAMO-ConvAI/bird](https://bird-bench.github.io/) repository or its Hugging
Face mirror) and prepare three artifacts:

| Artifact | Expected content |
| --- | --- |
| `dev.json` | one entry per question with `question_id`, `db_id`, `question`, `evidence`, `SQL` (gold), `difficulty` |
| `dev_tables.json` | per-database schema with `db_id`, `table_names_original`, `column_names_original`, `foreign_keys`, `primary_keys`, ... |
| `dev_databases/` | one directory per `db_id`, each containing `<db_id>.sqlite` and `database_description/` |

Note: the code refers to the BIRD dev split as the "test" split (see `--mode test` below),
because under the offline-only dev protocol the dev set is the evaluation split. Map the three
artifacts onto the environment variables:

```bash
export DATABASE_DIR=/path/to/dev_databases      # content of dev_databases/
export TABLES_FILE_PATH=/path/to/dev_tables.json
export TEST_FILE_PATH=/path/to/dev.json
```

The gold SQL used by `--gold` is the `SQL` field already present in `dev.json`, so no separate
gold file is required.

## Setup

Use Python 3.11 or newer.

```bash
pip install -r requirements.txt
pip install pyserini==1.2.0 --ignore-installed jsonschema
```

Copy the environment template and fill in local paths and service endpoints:

```bash
cp .env.example .env
```

Required dataset variables:

```bash
export DATABASE_DIR=/path/to/test_databases
export TABLES_FILE_PATH=/path/to/test_tables.json
export TEST_FILE_PATH=/path/to/test.json
```

## Preprocessing

Build the database content index and offline schema cache:

```bash
# Java is required by Pyserini indexing.
python3 preprocess/nltk_downloader.py

python3 preprocess/build_contents_index.py \
  --db-path $DATABASE_DIR \
  --index-save-folder db_contents_index

python3 preprocess/process_dataset.py \
  --input_data_file $TEST_FILE_PATH \
  --output_data_file test_bird_schema_full.json \
  --db_path $DATABASE_DIR \
  --tables $TABLES_FILE_PATH \
  --source bird \
  --mode test \
  --value_limit_num 2 \
  --db_content_index_path db_contents_index/
```

If the schema cache path differs from `test_bird_schema_full.json`, set:

```bash
OFFLINE_SCHEMA_PATH=/path/to/schema_cache.json
```

Build xmschema files for the reward model:

```bash
python3 preprocess/export_xmschema.py \
  --table-file-path $TABLES_FILE_PATH \
  --database-dir $DATABASE_DIR
```

## Running

### Generation setting (reported 73.1% EX)

This setting uses only the SFT/RL-trained SQLGovernor generation model; Gemini is not part of
the default profiles. Phase 1 samples 16 candidates per question, after which candidate
selection combines execution-result agreement with the pairwise reward model and selection
gates. The reported 73.1% is the final result of this generation/candidate-selection setting;
the full repair/refinement phases below are not run.

Generate candidates:

```bash
mkdir -p experiments/generation
python3 scripts/sql_pipeline_v2.py phase1 \
  --input $TEST_FILE_PATH \
  --output experiments/generation/generation.json \
  --max-workers 8
```

Run the selection stack directly on the Phase-1 candidates (the option name `--phase5-run` is a
legacy name and accepts any pipeline candidate JSON):

```bash
bash scripts/run_all_with_selection.sh \
  --phase5-run experiments/generation/generation.json \
  --db-root $DATABASE_DIR \
  --pair-rm-max-thread 8 \
  --qwen-model-key qwen3-235b \
  --qwen-max-workers 8 \
  --timeout-selector-max-workers 4
```

The final prediction map is
`experiments/generation/generation.json.unified_final.map.json`. Evaluate it as shown in
[Evaluation (EX)](#evaluation-ex).

### Full workflow setting (reported 73.9% EX)

This setting runs generation → validation → execution-feedback diagnosis → repair →
structural/rule-guided refinement → candidate selection:

```text
run_all_with_selection.sh -> sql_pipeline_v2.py all -> pair_rm -> structural support gate -> timeout specialists -> unified final map
```

Smoke test:

```bash
export OUTPUT_FOLDER=test_smoke
mkdir -p experiments/${OUTPUT_FOLDER} logs/run_all

nohup bash scripts/run_all_with_selection.sh \
  --input $TEST_FILE_PATH \
  --output experiments/${OUTPUT_FOLDER}/${OUTPUT_FOLDER}.json \
  --db-root $DATABASE_DIR \
  --pair-rm-max-thread 8 \
  --qwen-model-key qwen3-235b \
  --qwen-max-workers 8 \
  --timeout-selector-max-workers 4 \
  -- \
  --start-index 0 \
  --sample-count 4 \
  --max-workers 8 \
  --phase2-threads 12 \
  --phase3-threads 10 \
  --phase3-timeout 120 \
  --phase4-threads 10 \
  --phase4-max-attempts 3 \
  --phase4-exec-timeout 120 \
  --phase4-empty-stage2-model-key qwen3-235b \
  --phase5-threads 10 \
  --phase5-rules-preset refined_rules_depth1 \
  --phase5-regen-candidate-count 16 \
  --phase5-regen-batch-size 1 \
  --phase5-regen-max-rounds 16 \
  --phase5-regen-timeout 180 \
  --phase5-regen-max-tokens 8196 \
  > logs/run_all/${OUTPUT_FOLDER}.log 2>&1 &

echo $! > logs/run_all/${OUTPUT_FOLDER}.pid
```

Full run (adds `--resume`):

```bash
export OUTPUT_FOLDER=test
mkdir -p experiments/${OUTPUT_FOLDER} logs/run_all

nohup bash scripts/run_all_with_selection.sh \
  --input $TEST_FILE_PATH \
  --output experiments/${OUTPUT_FOLDER}/${OUTPUT_FOLDER}.json \
  --db-root $DATABASE_DIR \
  --pair-rm-max-thread 8 \
  --qwen-model-key qwen3-235b \
  --qwen-max-workers 8 \
  --timeout-selector-max-workers 4 \
  -- \
  --max-workers 8 \
  --phase2-threads 12 \
  --phase3-threads 10 \
  --phase3-timeout 120 \
  --phase4-threads 10 \
  --phase4-max-attempts 3 \
  --phase4-exec-timeout 120 \
  --phase4-empty-stage2-model-key qwen3-235b \
  --phase5-threads 10 \
  --phase5-rules-preset refined_rules_depth1 \
  --phase5-regen-candidate-count 16 \
  --phase5-regen-batch-size 1 \
  --phase5-regen-max-rounds 16 \
  --phase5-regen-timeout 180 \
  --phase5-regen-max-tokens 8196 \
  --resume \
  > logs/run_all/${OUTPUT_FOLDER}.log 2>&1 &

echo $! > logs/run_all/${OUTPUT_FOLDER}.pid
```

Main outputs:

- pipeline JSON: `experiments/${OUTPUT_FOLDER}/${OUTPUT_FOLDER}.json`
- phase snapshots: `*.phase1.json` through `*.phase5.json`
- structural gate output: `*.structural_support_selector.json`
- final SQL map: `*.phase5.json.unified_final.map.json`
- optional execution summary: `*.phase5.json.unified_final.exec_match.txt`

Checkpoint 2 reuses Checkpoint 1 artifacts and runs selection on the Phase 4 snapshot:

```bash
export OUTPUT_FOLDER=test
mkdir -p logs/run_all

nohup bash scripts/run_all_with_selection.sh \
  --phase5-run experiments/${OUTPUT_FOLDER}/${OUTPUT_FOLDER}.phase4.json \
  --phase3-run experiments/${OUTPUT_FOLDER}/${OUTPUT_FOLDER}.phase3.json \
  --db-root $DATABASE_DIR \
  --pair-rm-max-thread 8 \
  --qwen-model-key qwen3-235b \
  --qwen-max-workers 8 \
  --timeout-selector-max-workers 4 \
  > logs/run_all/${OUTPUT_FOLDER}.checkpoint2.log 2>&1 &

echo $! > logs/run_all/${OUTPUT_FOLDER}.checkpoint2.pid
```

## Evaluation (EX)

Compute execution accuracy after either setting with:

```bash
python3 scripts/eval_exec_match_map.py \
  --pred /path/to/unified_final.map.json \
  --gold $TEST_FILE_PATH \
  --db-root $DATABASE_DIR
```

The evaluator executes the final selected SQL and gold SQL and reports per-difficulty and overall
EX. It evaluates the complete gold question set: missing or empty predictions and SQL execution
failures remain in the denominator and count as incorrect.

The gold file is a JSON list with the same schema as BIRD `dev.json` (`question_id`, `db_id`,
`SQL`, `difficulty`), so passing `--gold $TEST_FILE_PATH` is sufficient.

**Expected output.** The script prints per-difficulty EX (simple / moderate / challenging) and an
overall EX. The reported values (73.1% generation, 73.9% full workflow) assume the paper's own
trained generation and reward models; with substitute models the pipeline still runs and reports
EX, but the numbers will differ.
