#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON3="python3"
if [[ ! -x "$PYTHON3" ]]; then
  PYTHON3="python3"
fi

INPUT=""
OUTPUT=""
DB_ROOT=""
GOLD=""
PHASE5_RUN_OVERRIDE=""
PHASE3_RUN_OVERRIDE=""
PHASE5_RUN=""
PHASE3_SNAPSHOT=""

PAIR_RM_MAX_THREAD=8
SCHEMA_PARSER="sqlglot"
QWEN_MODEL_KEY="qwen3-235b"
QWEN_MAX_WORKERS=8
TIMEOUT_SELECTOR_MAX_WORKERS=4

PIPELINE_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input) INPUT="$2"; shift 2 ;;
    --output) OUTPUT="$2"; shift 2 ;;
    --db-root) DB_ROOT="$2"; shift 2 ;;
    --gold) GOLD="$2"; shift 2 ;;
    --phase5-run) PHASE5_RUN_OVERRIDE="$2"; shift 2 ;;
    --phase3-run) PHASE3_RUN_OVERRIDE="$2"; shift 2 ;;
    --pair-rm-max-thread) PAIR_RM_MAX_THREAD="$2"; shift 2 ;;
    --schema-parser) SCHEMA_PARSER="$2"; shift 2 ;;
    --qwen-model-key) QWEN_MODEL_KEY="$2"; shift 2 ;;
    --qwen-max-workers) QWEN_MAX_WORKERS="$2"; shift 2 ;;
    --timeout-selector-max-workers) TIMEOUT_SELECTOR_MAX_WORKERS="$2"; shift 2 ;;
    --)
      shift
      PIPELINE_ARGS=("$@")
      break
      ;;
    *)
      echo "[ERROR] Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$DB_ROOT" ]]; then
  echo "Usage: $0 --db-root <db_root> (--input <samples_json> --output <pipeline_output.json> | --phase5-run <phase5_json>) [--gold <gold_json>] [selection opts] -- [sql_pipeline_v2 all args]" >&2
  exit 1
fi

if [[ -n "${PHASE5_RUN_OVERRIDE-}" ]]; then
  if [[ ! -f "${PHASE5_RUN_OVERRIDE}" ]]; then
    echo "[ERROR] --phase5-run file not found: $PHASE5_RUN_OVERRIDE" >&2
    exit 1
  fi
else
  if [[ -z "$INPUT" || -z "$OUTPUT" ]]; then
    echo "Usage: $0 --db-root <db_root> (--input <samples_json> --output <pipeline_output.json> | --phase5-run <phase5_json>) [--gold <gold_json>] [selection opts] -- [sql_pipeline_v2 all args]" >&2
    exit 1
  fi
  if [[ "$OUTPUT" != *.json ]]; then
    echo "[ERROR] --output must end with .json so phase snapshots can be derived." >&2
    exit 1
  fi
fi

if [[ -n "${PHASE5_RUN_OVERRIDE-}" ]]; then
  PHASE5_RUN="$PHASE5_RUN_OVERRIDE"
  if [[ -n "${PHASE3_RUN_OVERRIDE-}" ]]; then
    PHASE3_SNAPSHOT="$PHASE3_RUN_OVERRIDE"
  elif [[ "$PHASE5_RUN" == *.phase5.json ]]; then
    PHASE3_SNAPSHOT="${PHASE5_RUN%.phase5.json}.phase3.json"
  else
    PHASE3_SNAPSHOT=""
  fi
  PHASE5_SNAPSHOT="$PHASE5_RUN"
else
  PHASE3_SNAPSHOT="${OUTPUT%.json}.phase3.json"
  PHASE5_SNAPSHOT="${OUTPUT%.json}.phase5.json"
  PHASE5_RUN=""
fi

PAIR_RM_CHECKPOINT="${PHASE5_SNAPSHOT}_${SCHEMA_PARSER}_selection.checkpoint.jsonl"
PAIR_RM_FINAL_JSON="${PHASE5_SNAPSHOT}_${SCHEMA_PARSER}_final.json"
GENERAL_GATE_QIDS="${PHASE5_SNAPSHOT}.general_gate_qids.txt"
STRUCTURE_SELECTOR_OUT="${PHASE5_SNAPSHOT}.structural_support_selector.json"
TIMEOUT_RISK_QIDS="${PHASE5_SNAPSHOT}.timeout_risk_qids.txt"
TIMEOUT_ROUTER_OUT="${PHASE5_SNAPSHOT}.timeout_router.json"
TIMEOUT_PROBE_OUT="${PHASE5_SNAPSHOT}.timeout_probe.json"
UNIFIED_JSONL="${PHASE5_SNAPSHOT}.unified_selection.jsonl"
UNIFIED_FINAL_JSON="${PHASE5_SNAPSHOT}.unified_final.json"
UNIFIED_FINAL_MAP="${PHASE5_SNAPSHOT}.unified_final.map.json"
UNIFIED_EVAL="${PHASE5_SNAPSHOT}.unified_final.exec_match.txt"

echo "========================================"
echo " run_all_with_selection.sh"
echo " $(date '+%Y-%m-%d %H:%M:%S %z')"
echo "========================================"
echo "[INFO] INPUT                = ${INPUT:-<skipped>}"
echo "[INFO] OUTPUT               = ${OUTPUT:-<skipped>}"
echo "[INFO] PHASE5_RUN_OVERRIDE  = ${PHASE5_RUN_OVERRIDE-<none>}"
echo "[INFO] PHASE3_RUN_OVERRIDE  = ${PHASE3_RUN_OVERRIDE-<auto>}"
echo "[INFO] DB_ROOT              = $DB_ROOT"
echo "[INFO] GOLD                = ${GOLD:-<none>}"
echo "[INFO] QWEN_MODEL_KEY       = $QWEN_MODEL_KEY"
echo "[INFO] SCHEMA_PARSER        = $SCHEMA_PARSER"
echo "[INFO] PAIR_RM_MAX_THREAD   = $PAIR_RM_MAX_THREAD"
echo "[INFO] PIPELINE_ARGS        = ${PIPELINE_ARGS[*]:-<none>}"

if [[ -z "${PHASE5_RUN_OVERRIDE-}" ]]; then
  echo
  echo "[STEP 1] Run sql_pipeline_v2.py all ..."
  SAVE_PHASE_SNAPSHOT=true \
  "$PYTHON3" "${PROJECT_ROOT}/scripts/sql_pipeline_v2.py" all \
    --input "$INPUT" \
    --output "$OUTPUT" \
    "${PIPELINE_ARGS[@]}"

  if [[ -f "$PHASE5_SNAPSHOT" ]]; then
    PHASE5_RUN="$PHASE5_SNAPSHOT"
  else
    PHASE5_RUN="$OUTPUT"
  fi
else
  echo
  echo "[STEP 1] Skip pipeline and reuse existing phase5 run ..."
  echo "[INFO] phase5_run = $PHASE5_RUN"
fi

if [[ -n "$PHASE3_SNAPSHOT" && ! -f "$PHASE3_SNAPSHOT" ]]; then
  echo "[WARN] phase3 snapshot not found: $PHASE3_SNAPSHOT"
  PHASE3_SNAPSHOT=""
fi

echo
echo "[STEP 2] Run pair_rm ..."
"$PYTHON3" "${PROJECT_ROOT}/src/selection/pair_rm.py" \
  --file "$PHASE5_RUN" \
  --max-thread "$PAIR_RM_MAX_THREAD" \
  --schema-parser "$SCHEMA_PARSER" \
  --output-suffix "_${SCHEMA_PARSER}" \
  --resume

if [[ ! -f "$PAIR_RM_CHECKPOINT" ]]; then
  echo "[ERROR] Missing pair_rm checkpoint: $PAIR_RM_CHECKPOINT" >&2
  exit 1
fi

echo
echo "[STEP 3] Build low-confidence qid list for general structural gate ..."
"$PYTHON3" - <<PY
import json
from pathlib import Path
run=Path("${PHASE5_RUN}")
ckpt=Path("${PAIR_RM_CHECKPOINT}")
out=Path("${GENERAL_GATE_QIDS}")
with run.open() as f:
    run_payload=json.load(f)
difficulty={int(r["question_id"]): str(r.get("difficulty","unknown")).strip().lower() for r in run_payload["results"]}
def gap(vals):
    vals=[float(x) for x in (vals or [])]
    if not vals:
        return 0.0
    vals=sorted(vals, reverse=True)
    best=vals[0]
    second=vals[1] if len(vals)>1 else best
    return best-second
def group_count(group_ids):
    return len({int(g) for g in (group_ids or []) if int(g) != -1})
qids=[]
with ckpt.open() as f:
    for line in f:
        line=line.strip()
        if not line:
            continue
        row=json.loads(line)
        qid=int(row["question_id"])
        if (
            group_count(row.get("group_id_list")) > 1
            and gap(row.get("merged_score")) <= 0.0
            and gap(row.get("maj_score_refine")) <= 0.0
            and gap(row.get("rm_score")) <= 0.0
        ):
            qids.append(qid)
out.write_text("\\n".join(map(str, qids)) + "\\n", encoding="utf-8")
print(f"wrote {out} with {len(qids)} qids")
PY

echo
echo "[STEP 4] Run structural support gate ..."
"$PYTHON3" "${PROJECT_ROOT}/scripts/apply_structural_support_gate.py" \
  --run "$PHASE5_RUN" \
  --checkpoint "$PAIR_RM_CHECKPOINT" \
  --qid-list "$GENERAL_GATE_QIDS" \
  --output "$STRUCTURE_SELECTOR_OUT"

echo
echo "[STEP 5] Build timeout-risk qid list from phase3 snapshot ..."
if [[ -n "$PHASE3_SNAPSHOT" ]]; then
"$PYTHON3" - <<PY
import json
from pathlib import Path
src=Path("${PHASE3_SNAPSHOT}")
out=Path("${TIMEOUT_RISK_QIDS}")
with src.open() as f:
    payload=json.load(f)
qids=[]
for sample in payload.get("results") or []:
    qid=int(sample["question_id"])
    if any((cand.get("execution") or {}).get("status") == "timeout" for cand in sample.get("sql_candidates") or []):
        qids.append(qid)
out.write_text("\\n".join(map(str, qids)) + "\\n", encoding="utf-8")
print(f"wrote {out} with {len(qids)} qids")
PY
else
  printf "" > "$TIMEOUT_RISK_QIDS"
  echo "[INFO] phase3 snapshot unavailable; wrote empty timeout-risk qid list to $TIMEOUT_RISK_QIDS"
fi

echo
echo "[STEP 6] Run timeout specialists ..."
if [[ -n "$PHASE3_SNAPSHOT" ]]; then
  "$PYTHON3" "${PROJECT_ROOT}/scripts/experiment_timeout_risk_router.py" \
    --run-final "$PHASE5_RUN" \
    --run-timeout-source "$PHASE3_SNAPSHOT" \
    --qid-list "$TIMEOUT_RISK_QIDS" \
    --db-root "$DB_ROOT" \
    --output "$TIMEOUT_ROUTER_OUT" \
    --model-key "$QWEN_MODEL_KEY" \
    --timeout 30 \
    --max-workers "$TIMEOUT_SELECTOR_MAX_WORKERS" \
    --max-timeout-repairs 2
  "$PYTHON3" "${PROJECT_ROOT}/scripts/experiment_timeout_probe_selector.py" \
    --run "$PHASE5_RUN" \
    --qid-list "$TIMEOUT_RISK_QIDS" \
    --db-root "$DB_ROOT" \
    --output "$TIMEOUT_PROBE_OUT" \
    --model-key "$QWEN_MODEL_KEY" \
    --timeout 30 \
    --max-workers "$TIMEOUT_SELECTOR_MAX_WORKERS"
else
  printf '{"results": []}\n' > "$TIMEOUT_ROUTER_OUT"
  printf '{"results": []}\n' > "$TIMEOUT_PROBE_OUT"
  echo "[INFO] phase3 snapshot unavailable; skipped timeout specialists"
fi

echo
echo "[STEP 7] Apply unified gate ..."
UNIFIED_GATE_CMD=(
  "$PYTHON3" "${PROJECT_ROOT}/scripts/apply_pair_rm_qwen_timeout_gate.py"
  --run "$PHASE5_RUN"
  --checkpoint "$PAIR_RM_CHECKPOINT"
  --selector-output "$STRUCTURE_SELECTOR_OUT"
  --timeout-router-output "$TIMEOUT_ROUTER_OUT"
  --timeout-probe-output "$TIMEOUT_PROBE_OUT"
  --timeout-risk-qids "$TIMEOUT_RISK_QIDS"
  --output-jsonl "$UNIFIED_JSONL"
  --output-final-json "$UNIFIED_FINAL_JSON"
  --output-final-map-json "$UNIFIED_FINAL_MAP"
  --general-difficulties all
  --max-merged-gap 0
  --max-maj-gap 0
  --max-rm-gap 0
  --max-cluster-count 6
  --max-top-size 8
  --timeout-max-cluster-count 0
)
if [[ -n "$PHASE3_SNAPSHOT" ]]; then
  UNIFIED_GATE_CMD+=(--timeout-risk-source-run "$PHASE3_SNAPSHOT")
fi
"${UNIFIED_GATE_CMD[@]}"

if [[ -n "$GOLD" ]]; then
  echo
  echo "[STEP 8] Evaluate unified final map ..."
  "$PYTHON3" "${PROJECT_ROOT}/scripts/eval_exec_match_map.py" \
    --pred "$UNIFIED_FINAL_MAP" \
    --gold "$GOLD" \
    --db-root "$DB_ROOT" \
    --workers 8 \
    --timeout 120 \
    --output "$UNIFIED_EVAL"
fi

echo
echo "[DONE]"
echo "[INFO] phase5_run           = $PHASE5_RUN"
echo "[INFO] pair_rm_checkpoint   = $PAIR_RM_CHECKPOINT"
echo "[INFO] structural_selector  = $STRUCTURE_SELECTOR_OUT"
echo "[INFO] timeout_router       = $TIMEOUT_ROUTER_OUT"
echo "[INFO] timeout_probe        = $TIMEOUT_PROBE_OUT"
echo "[INFO] unified_final_json   = $UNIFIED_FINAL_JSON"
echo "[INFO] unified_final_map    = $UNIFIED_FINAL_MAP"
if [[ -n "$GOLD" ]]; then
  echo "[INFO] unified_eval         = $UNIFIED_EVAL"
fi
