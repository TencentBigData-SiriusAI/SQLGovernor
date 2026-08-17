"""Global settings loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)


class Settings:
    """Runtime settings for the SQL pipeline."""

    PROJECT_ROOT: Path = Path(__file__).parent.parent
    DATA_DIR: Path = PROJECT_ROOT / "data"
    EXPERIMENT_DIR: Path = PROJECT_ROOT / "experiments"
    LOG_DIR: Path = PROJECT_ROOT / "logs"

    DEFAULT_MODEL: str = os.getenv("DEFAULT_MODEL", "sql-generation-model")
    SQL_GENERATION_MODEL: str = os.getenv("SQL_GENERATION_MODEL", "sql-generation-model")
    MODEL_TEMPERATURE: float = float(os.getenv("MODEL_TEMPERATURE", "0.7"))
    MAX_TOKENS: int = int(os.getenv("MAX_TOKENS", "8192"))
    SQL_GENERATION_MAX_TOKENS: int = int(os.getenv("SQL_GENERATION_MAX_TOKENS", "12000"))
    SQL_SELECTION_MAX_TOKENS: int = int(os.getenv("SQL_SELECTION_MAX_TOKENS", "12000"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "1440"))
    MAX_RETRY_ATTEMPTS: int = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    SQL_GENERATION_VOTE_COUNT: int = int(os.getenv("SQL_GENERATION_VOTE_COUNT", "32"))
    SQL_GENERATION_VOTE_WORKERS: int = int(os.getenv("SQL_GENERATION_VOTE_WORKERS", "1"))
    SQL_GENERATION_BATCH_SIZE: int = int(os.getenv("SQL_GENERATION_BATCH_SIZE", "32"))
    SQL_GENERATION_MAX_RETRIES: int = int(os.getenv("SQL_GENERATION_MAX_RETRIES", "3"))
    SQL_GENERATION_RETRY_BACKOFF: float = float(os.getenv("SQL_GENERATION_RETRY_BACKOFF", "2.0"))
    SQL_GENERATION_REQUEST_TIMEOUT: int = int(os.getenv("SQL_GENERATION_REQUEST_TIMEOUT", "180"))
    SQL_GENERATION_EMPTY_RETRY_LIMIT: int = int(os.getenv("SQL_GENERATION_EMPTY_RETRY_LIMIT", "2"))
    SQL_CORRECTION_MODEL: str = os.getenv("SQL_CORRECTION_MODEL") or SQL_GENERATION_MODEL
    GRAPH_RECURSION_LIMIT: int = int(os.getenv("GRAPH_RECURSION_LIMIT", "200"))

    SQL_RESULT_VOTING_ENABLED: bool = (
        os.getenv("SQL_RESULT_VOTING_ENABLED", "true").lower() == "true"
    )
    SQL_RESULT_VOTING_TIMEOUT: int = int(os.getenv("SQL_RESULT_VOTING_TIMEOUT", "8"))
    SQL_RESULT_VOTING_TRIPLE_LIMIT: int = int(os.getenv("SQL_RESULT_VOTING_TRIPLE_LIMIT", "16"))
    SQL_RESULT_VOTING_WEIGHTS: str = os.getenv("SQL_RESULT_VOTING_WEIGHTS", "")
    SQL_RESULT_VOTING_WORKERS: int = int(os.getenv("SQL_RESULT_VOTING_WORKERS", "8"))
    SQL_RESULT_VOTING_AUTOCORRECT_ENABLED: bool = (
        os.getenv("SQL_RESULT_VOTING_AUTOCORRECT_ENABLED", "true").lower() == "true"
    )
    SQL_RESULT_VOTING_AUTOCORRECT_MAX_ATTEMPTS: int = int(
        os.getenv("SQL_RESULT_VOTING_AUTOCORRECT_MAX_ATTEMPTS", "1")
    )

    SQL_GENERATION_SAVE_RESULTS: bool = (
        os.getenv("SQL_GENERATION_SAVE_RESULTS", "false").lower() == "true"
    )
    SQL_GENERATION_SAVE_DIR: str = os.getenv(
        "SQL_GENERATION_SAVE_DIR",
        str((PROJECT_ROOT / "logs" / "generation_results").resolve()),
    )
    SQL_GENERATION_PRIMARY_TEMPERATURE: float = float(
        os.getenv(
            "SQL_GENERATION_PRIMARY_TEMPERATURE",
            os.getenv("MODEL_TEMPERATURE", "1.0"),
        )
    )

    SQL_GEN_PROFILE_SIMPLE: str = os.getenv("SQL_GEN_PROFILE_SIMPLE", "")
    SQL_GEN_PROFILE_MODERATE: str = os.getenv("SQL_GEN_PROFILE_MODERATE", "")
    SQL_GEN_PROFILE_CHALLENGING: str = os.getenv("SQL_GEN_PROFILE_CHALLENGING", "")

    RM_BASE_URL: str = os.getenv("RM_BASE_URL", "http://0.0.0.0:8082/v1")
    RM_MODEL_NAME: str = os.getenv("RM_MODEL_NAME", "SQL-SELECTION-MODEL")
    RM_TOKENIZER: str = os.getenv("RM_TOKENIZER", "your-org/sql-selection-32b")
    RM_MAX_RESULTS_TOKENS: int = int(os.getenv("RM_MAX_RESULTS_TOKENS", "256"))
    RM_BATCH_SIZE: int = int(os.getenv("RM_BATCH_SIZE", "1"))
    RM_MAX_WORKERS: int = int(os.getenv("RM_MAX_WORKERS", "8"))
    RM_MAX_RETRY_ATTEMPTS: int = int(os.getenv("RM_MAX_RETRY_ATTEMPTS", "3"))
    RM_RETRY_BACKOFF: float = float(os.getenv("RM_RETRY_BACKOFF", "1.0"))
    SQL_SELECTION_ALPHA: float = float(os.getenv("SQL_SELECTION_ALPHA", "0.5"))
    SQL_SELECTION_NORMALIZE: str = os.getenv("SQL_SELECTION_NORMALIZE", "identity")
    ENABLE_SQL_SELECTION_BLEND: bool = (
        os.getenv("ENABLE_SQL_SELECTION_BLEND", "true").lower() == "true"
    )

    DATABASE_DIR: Optional[str] = os.getenv("DATABASE_DIR", "data/databases")

    _offline_schema_env = os.getenv("OFFLINE_SCHEMA_PATH", "test_bird_schema_full.json")
    OFFLINE_SCHEMA_PATH: Optional[Path] = Path(_offline_schema_env)

    _few_shot_path = os.getenv("FEW_SHOT_FILE", "")
    if _few_shot_path:
        FEW_SHOT_FILE: Optional[Path] = Path(_few_shot_path)
    else:
        _default_few_shot = PROJECT_ROOT / "data" / "few_shots.json"
        FEW_SHOT_FILE = _default_few_shot if _default_few_shot.exists() else None

    _few_shot_embeddings = os.getenv("FEW_SHOT_EMBEDDINGS_FILE", "")
    if _few_shot_embeddings:
        FEW_SHOT_EMBEDDINGS_FILE: Optional[Path] = Path(_few_shot_embeddings)
    else:
        _default_embeddings = PROJECT_ROOT / "data" / "few_shot_embeddings.npy"
        FEW_SHOT_EMBEDDINGS_FILE = _default_embeddings if _default_embeddings.exists() else None

    FEW_SHOT_TOP_K: int = int(os.getenv("FEW_SHOT_TOP_K", "5"))
    FEW_SHOT_MIN_SIMILARITY: float = float(os.getenv("FEW_SHOT_MIN_SIMILARITY", "0.0"))
    FEW_SHOT_MODE: str = os.getenv("FEW_SHOT_MODE", "embedding").lower()
    FEW_SHOT_EMBEDDING_MODEL: str = os.getenv("FEW_SHOT_EMBEDDING_MODEL", "")
    FEW_SHOT_EMBEDDING_ENDPOINT: str = os.getenv("FEW_SHOT_EMBEDDING_ENDPOINT", "")
    FEW_SHOT_EMBEDDING_API_KEY: str = os.getenv("FEW_SHOT_EMBEDDING_API_KEY", "")
    if FEW_SHOT_EMBEDDING_API_KEY.startswith("${") and FEW_SHOT_EMBEDDING_API_KEY.endswith("}"):
        key_name = FEW_SHOT_EMBEDDING_API_KEY[2:-1]
        FEW_SHOT_EMBEDDING_API_KEY = os.getenv(key_name, "")
    FEW_SHOT_EMBEDDING_DIM: int = int(os.getenv("FEW_SHOT_EMBEDDING_DIM", "1024"))
    FEW_SHOT_EMBEDDING_TIMEOUT: int = int(os.getenv("FEW_SHOT_EMBEDDING_TIMEOUT", "120"))
    FEW_SHOT_MAX_TEXT_LENGTH: int = int(os.getenv("FEW_SHOT_MAX_TEXT_LENGTH", "32000"))

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/bird.log")
    ENABLE_TOKEN_TRACKING: bool = os.getenv("ENABLE_TOKEN_TRACKING", "true").lower() == "true"
    TOKEN_LOG_DIR: str = os.getenv("TOKEN_LOG_DIR", "logs/token_tracking")

    EXPERIMENT_OUTPUT_DIR: str = os.getenv("EXPERIMENT_OUTPUT_DIR", "experiments/bird")
    SAVE_INTERMEDIATE_RESULTS: bool = (
        os.getenv("SAVE_INTERMEDIATE_RESULTS", "true").lower() == "true"
    )
    SQL_PROJECTION_RULES_ENABLED: bool = (
        os.getenv("SQL_PROJECTION_RULES_ENABLED", "true").lower() == "true"
    )
    SAVE_EXECUTION_RESULTS: bool = (
        os.getenv("SAVE_EXECUTION_RESULTS", "true").lower() == "true"
    )

    @classmethod
    def ensure_directories(cls) -> None:
        """Create output directories used by the pipeline."""

        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
        cls.EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
        Path(cls.TOKEN_LOG_DIR).mkdir(parents=True, exist_ok=True)
        Path(cls.EXPERIMENT_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_database_path(cls, database_name: str) -> Path:
        """Return the SQLite file path for a database id."""

        if cls.DATABASE_DIR:
            db_dir = Path(cls.DATABASE_DIR) / database_name
            sqlite_files = list(db_dir.glob("*.sqlite"))
            if sqlite_files:
                return sqlite_files[0]
        raise FileNotFoundError(f"Database {database_name} not found")


Settings.ensure_directories()
