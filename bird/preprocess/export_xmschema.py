#!/usr/bin/env python3
"""Generate .xmschema files for BIRD databases."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import Settings
from src.tools.database import generate_mschema_str


def export_single(sqlite_path: Path, force: bool = False, table_file_path: str = "") -> bool:
    """Generate a .xmschema file next to the provided SQLite file."""

    if not sqlite_path.exists():
        logger.warning(f"SQLite file missing: {sqlite_path}")
        return False

    target_path = sqlite_path.with_suffix(".xmschema")
    if target_path.exists() and not force:
        logger.info(f"{target_path} already exists (use --force to overwrite)")
        return False

    logger.info(f"Generating {target_path}")
    mschema_text = generate_mschema_str(str(sqlite_path), table_file_path)
    target_path.write_text(mschema_text, encoding="utf-8")
    logger.info(f"Generated: {target_path}")
    return True


def discover_sqlite_paths(database_dir: Path, db_id: str | None) -> list[Path]:
    if db_id:
        candidate = database_dir / db_id / f"{db_id}.sqlite"
        return [candidate]

    paths: list[Path] = []
    for child in sorted(database_dir.iterdir()):
        if not child.is_dir():
            continue
        candidate = child / f"{child.name}.sqlite"
        if candidate.exists():
            paths.append(candidate)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate .xmschema files")
    parser.add_argument(
        "--database-dir",
        default=Settings.DATABASE_DIR,
        help="Directory containing SQLite databases (default: config.settings DATABASE_DIR)",
    )
    parser.add_argument(
        "--db-id",
        help="Only export .xmschema for a single db_id (optional)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing .xmschema files",
    )
    parser.add_argument(
        "--table-file-path",
        default=os.getenv("TABLE_FILE_PATH", "dev_tables.json"),
        help="Path to the tables.json description file",
    )
    args = parser.parse_args()

    database_dir = Path(args.database_dir).expanduser().resolve()
    if not database_dir.exists():
        raise FileNotFoundError(f"Database dir not found: {database_dir}")

    sqlite_paths = discover_sqlite_paths(database_dir, args.db_id)
    if not sqlite_paths:
        logger.warning(f"No .sqlite files found under {database_dir}")
        return

    generated = 0
    for sqlite_path in sqlite_paths:
        if export_single(sqlite_path, force=args.force, table_file_path=args.table_file_path):
            generated += 1

    logger.info(f"Done: generated {generated} .xmschema files")


if __name__ == "__main__":
    main()
