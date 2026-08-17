# SQLGovernor

This is the artifact repository for the paper:

> **SQLGovernor: An LLM-Powered Framework for SQL Lifecycle Governance**
>
> Submitted to VLDB 2027

## About the Paper

Enterprise SQL development faces persistent challenges: syntax errors from dialect migration,
logic flaws that escape code review, performance anti-patterns that degrade at scale, and
fragmented toolchains with no shared context. **SQLGovernor** addresses these with a unified
LLM-powered framework that governs the full SQL lifecycle — from code completion and generation,
through syntax correction, to logic & efficiency diagnosis — all connected by a shared session
context and a dynamically maintained knowledge base.

## Artifact Availability

**PVLDB Artifact Availability:** The source code and data artifacts for this paper are made
available at this repository.

| Artifact | Status | Note |
| --- | --- | --- |
| BIRD dev reproduction pipeline | Available | Full pipeline and reproduction instructions in `bird/` |
| Micro-benchmark samples | Available (anonymized subset) | Covers the four core modules in `testcases/` |

## Reproducibility Scope

| Paper result | Reproducible from this repo? | Where |
| --- | --- | --- |
| BIRD dev — SQL generation (EX 73.1%) | code & pipeline | `bird/` |
| BIRD dev — full workflow (EX 73.9%) | code & pipeline | `bird/` |
| Micro-benchmark — four core modules | Samples only (format reference) | `testcases/` |

The four core modules are evaluated on a private, production-derived benchmark whose full data
cannot be released for privacy reasons. `testcases/` provides an anonymized subset that
illustrates each module's input/output format and evaluation rubric, but it is not the full
benchmark and is not accompanied by a scoring harness. The BIRD dev pipeline, in contrast, is
fully reproducible end-to-end via `bird/`; note that the trained generation and reward model
weights are not distributed, so reproducing the exact reported EX values requires supplying
models trained per the paper (see `bird/README.md`).

## Repository Layout

```
.
├── bird/        # BIRD dev reproduction pipeline (Sec. 8.3.3 / 8.6.2)
├── testcases/   # Anonymized micro-benchmark samples for the four core modules
├── LICENSE
└── README.md
```

## Micro-Benchmark

`testcases/` is an anonymized subset of the evaluation benchmark used in the paper, covering all
four core modules:

```
testcases/
├── code_completion/            # 10 FIM completion samples (JSON)
├── code_generation/            # 100 NL-to-SQL pairs (JSON)
├── syntax_correction/          # 10 annotated SQL cases
└── logic_efficiency_diagnosis/ # 70 annotated SQL cases across 7 categories
    ├── full_table_scan/
    ├── cartesian_product/
    ├── implicit_type_conversion/
    ├── empty_table/
    ├── basic_logic_flaws/
    ├── where_clause_logic/
    └── business_intent_mismatch/
```

| Module | Format | Size | Description |
|--------|--------|------|-------------|
| Code Completion | JSON (`prefix` + `suffix` + `ground_truth`) | 10 samples | FIM-style SQL completion with context and ground truth |
| Code Generation | JSON (`question` + `sql`) | 100 samples | Natural-language queries paired with gold SQL |
| Syntax Correction | Annotated `.sql` files | 10 samples | Erroneous SQL with problem descriptions and expected diagnoses |
| Logic & Efficiency Diagnosis | Annotated `.sql` files | 70 cases (7 categories × 10) | SQL anti-patterns with sub-cases, each annotated with root cause and fix |

> **Note:** All data has been anonymized. These samples illustrate the format and rubric; the
> full private benchmark and its scoring harness are not released.

## BIRD Benchmark Reproduction

The BIRD dev-set results reported in the paper (EX 73.1% for SQL generation, EX 73.9% for the
full workflow) can be reproduced with the pipeline in [`bird/`](bird/):

```text
Schema Analysis -> SQL Generation -> Candidate Dispatch -> Validation -> Correction -> Execution -> Result Selection
```

The `bird/` directory contains the full Text-to-SQL pipeline (candidate generation, validation,
correction, and result selection) together with the evaluation scripts used to obtain the
reported BIRD dev accuracy. See [`bird/README.md`](bird/README.md) for the dataset prerequisites,
model configuration, and step-by-step reproduction instructions.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
