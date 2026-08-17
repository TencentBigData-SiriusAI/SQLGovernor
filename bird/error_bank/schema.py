"""ErrorEntry schema and ErrorType definitions."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class ErrorType(str, Enum):
    """Coarse error taxonomy across all phases."""
    # Phase 2: schema validation
    NO_SUCH_TABLE = "no_such_table"
    NO_SUCH_COLUMN = "no_such_column"
    SCHEMA_VALIDATE_FAIL = "schema_validate_fail"
    SYNTAX_ERROR = "syntax_error"           # bracket/quote mismatch, missing FROM, etc.

    # Phase 3/4: execution errors
    EMPTY_RESULT = "empty_result"
    TIMEOUT = "timeout"
    RUNTIME_ERROR = "runtime_error"

    # Phase 4.5 probe: fine-grained empty-result subtypes
    VALUE_MISMATCH = "value_mismatch"       # FUZZY_MISMATCH, CASE_MISMATCH, SUFFIX_MISMATCH, etc.
    VALUE_NOT_EXISTS = "value_not_exists"
    JOIN_ERROR = "join_error"               # JOIN_NO_OVERLAP, wrong key
    HAVING_TOO_RESTRICTIVE = "having_too_restrictive"
    FUNCTION_FILTER_KILLS = "function_filter_kills"
    SUBQUERY_ERROR = "subquery_error"

    # Phase 5: rule-based structural errors
    RULE_VIOLATION = "rule_violation"

    UNKNOWN = "unknown"


class ErrorCategory(str, Enum):
    """Maps to index layers: which index layer handles this error type."""
    SCHEMA = "schema"           # A → I₂ exact index
    VALUE = "value"             # B → I₂ exact index + column facts
    STRUCTURAL = "structural"   # C → I₄ HNSW semantic index


# ErrorType → ErrorCategory mapping
ERROR_CATEGORY_MAP = {
    ErrorType.NO_SUCH_TABLE: ErrorCategory.SCHEMA,
    ErrorType.NO_SUCH_COLUMN: ErrorCategory.SCHEMA,
    ErrorType.SCHEMA_VALIDATE_FAIL: ErrorCategory.SCHEMA,
    ErrorType.SYNTAX_ERROR: ErrorCategory.SCHEMA,
    ErrorType.EMPTY_RESULT: ErrorCategory.VALUE,
    ErrorType.VALUE_MISMATCH: ErrorCategory.VALUE,
    ErrorType.VALUE_NOT_EXISTS: ErrorCategory.VALUE,
    ErrorType.JOIN_ERROR: ErrorCategory.VALUE,
    ErrorType.HAVING_TOO_RESTRICTIVE: ErrorCategory.STRUCTURAL,
    ErrorType.FUNCTION_FILTER_KILLS: ErrorCategory.VALUE,
    ErrorType.SUBQUERY_ERROR: ErrorCategory.STRUCTURAL,
    ErrorType.TIMEOUT: ErrorCategory.STRUCTURAL,
    ErrorType.RUNTIME_ERROR: ErrorCategory.STRUCTURAL,
    ErrorType.RULE_VIOLATION: ErrorCategory.STRUCTURAL,
    ErrorType.UNKNOWN: ErrorCategory.STRUCTURAL,
}


@dataclass
class DBFact:
    """A ground-truth fact obtained by querying the actual database."""
    column: str                              # "table.column"
    expected_value: Optional[str] = None     # what the SQL used
    actual_values: List[str] = field(default_factory=list)  # what the DB actually contains
    diagnosis: str = ""                      # CASE_MISMATCH, PREFIX_MATCH, etc.
    closest_match: Optional[str] = None
    suggested_fix: Optional[str] = None


@dataclass
class ErrorEntry:
    """A single error event grounded in execution facts.

    Corresponds to the formal definition:
    e = (q, s, S_e, φ_e, τ_e, δ_e, F_e, r_e)
    """
    # ── Context ──
    question_id: int
    db_id: str
    question: str = ""
    evidence: str = ""

    # ── Schema Anchor S_e ⊆ V ──
    tables: List[str] = field(default_factory=list)    # tables involved
    columns: List[str] = field(default_factory=list)   # "table.column" involved

    # ── SQL Signature φ_e ──
    sql_text: str = ""
    sql_signature: Dict[str, bool] = field(default_factory=dict)  # has_join, has_group, etc.

    # ── Error Info (τ_e, δ_e) ──
    error_type: ErrorType = ErrorType.UNKNOWN
    error_phase: str = ""                    # "phase2", "phase4", "phase4.5_probe", "phase5"
    error_detail: str = ""                   # human-readable description
    killer_condition: str = ""               # the WHERE/JOIN condition that kills results

    # ── DB Facts F_e (execution-grounded) ──
    db_facts: List[DBFact] = field(default_factory=list)

    # ── Fix Record r_e ──
    fix_sql: str = ""
    fix_succeeded: bool = False

    # ── Metadata ──
    wrong_names: List[str] = field(default_factory=list)  # for schema errors: the wrong table/column names
    matched_rules: List[str] = field(default_factory=list)  # for phase5: which rules triggered
    timestamp: float = 0.0

    @property
    def category(self) -> ErrorCategory:
        return ERROR_CATEGORY_MAP.get(self.error_type, ErrorCategory.STRUCTURAL)

    @property
    def schema_anchor_key(self) -> str:
        """Primary schema anchor for indexing: most specific column, or table."""
        if self.columns:
            return self.columns[0]
        if self.tables:
            return self.tables[0]
        return ""
