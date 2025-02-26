# SQLGovernor

This is the artifact repository for the paper:

> **SQLGovernor: An LLM-Powered Framework for SQL Lifecycle Governance**
>
> Submitted to VLDB 2027

## About the Paper

Enterprise SQL development faces persistent challenges: syntax errors from dialect migration, logic flaws that escape code review, performance anti-patterns that degrade at scale, and fragmented toolchains with no shared context. **SQLGovernor** addresses these with a unified LLM-powered framework that governs the full SQL lifecycle — from code completion and generation, through syntax correction, to logic & efficiency diagnosis — all connected by a shared session context and a dynamically maintained knowledge base.

## Micro-Benchmark

We open-source a subset of the evaluation benchmark used in the paper, covering all four core modules:

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
| Logic & Efficiency Diagnosis | Annotated `.sql` files | 70 cases (7 categories &times; 10) | SQL anti-patterns with sub-cases, each annotated with root cause and fix |

> **Note:** All data has been anonymized.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
