"""
error_bank/ — Execution-Grounded Hierarchical Error Memory (EG-HEM)

Architecture:
    error_bank/
    ├── __init__.py           # public API
    ├── schema.py             # ErrorEntry dataclass + ErrorType enum
    ├── store.py              # ErrorBankStore: 4-layer hybrid index (I₁~I₄)
    ├── collector.py          # collect errors from Phase 2/3/4/5 pipeline outputs
    ├── retriever.py          # hierarchical retrieval with specificity-weighted scoring
    ├── confidence.py         # Bayesian confidence tracking (Beta distribution)
    ├── propagation.py        # Schema-topology-aware error signal propagation (PPR)
    ├── injector.py           # confidence-gated prompt injection (WARN/HINT/EXAMPLE)
    └── prober.py             # progressive probing for empty-result diagnosis (moved from scripts/)

Usage in pipeline:
    # After Phase 4, before Phase 5:
    from error_bank import ErrorBankStore, collect_errors, retrieve_and_inject

    bank = ErrorBankStore()
    collect_errors(bank, phase2_data, phase3_data, phase4_data)  # populate from pipeline
    context = retrieve_and_inject(bank, question, db_id, tables, columns)  # query for correction
"""
from error_bank.schema import ErrorEntry, ErrorType
from error_bank.store import ErrorBankStore
from error_bank.collector import collect_from_pipeline
from error_bank.retriever import retrieve
from error_bank.injector import build_error_context
