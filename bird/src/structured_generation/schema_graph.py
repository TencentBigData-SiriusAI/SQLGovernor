"""Schema graph extraction helpers for structured generation."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from ..tools import get_database_schema

_GENERIC_KEY_NAMES = {"id"}
_KEYISH_SUFFIXES = ("_id", "code", "_code", "_api_id", "_fifa_api_id")
_NUMERIC_SUFFIX_RE = re.compile(r"^(.*?)(\d+)$")


@dataclass(slots=True)
class GraphEdge:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relation_type: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_table": self.from_table,
            "from_column": self.from_column,
            "to_table": self.to_table,
            "to_column": self.to_column,
            "relation_type": self.relation_type,
            "confidence": self.confidence,
        }


@dataclass(slots=True)
class SlotFamily:
    table: str
    family_name: str
    columns: list[str] = field(default_factory=list)
    family_type: str = "numeric_suffix"
    confidence: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "family_name": self.family_name,
            "columns": list(self.columns),
            "family_type": self.family_type,
            "confidence": self.confidence,
        }


@dataclass(slots=True)
class ColumnAmbiguity:
    concept: str
    columns: list[str] = field(default_factory=list)
    confidence: float = 0.5

    def to_dict(self) -> dict[str, Any]:
        return {
            "concept": self.concept,
            "columns": list(self.columns),
            "confidence": self.confidence,
        }


@dataclass(slots=True)
class SchemaGraph:
    tables: list[str]
    columns_by_table: dict[str, list[str]]
    edges: list[GraphEdge] = field(default_factory=list)
    bridge_tables: list[str] = field(default_factory=list)
    slot_families: list[SlotFamily] = field(default_factory=list)
    column_ambiguities: list[ColumnAmbiguity] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tables": list(self.tables),
            "columns_by_table": {key: list(values) for key, values in self.columns_by_table.items()},
            "edges": [edge.to_dict() for edge in self.edges],
            "bridge_tables": list(self.bridge_tables),
            "slot_families": [family.to_dict() for family in self.slot_families],
            "column_ambiguities": [item.to_dict() for item in self.column_ambiguities],
        }

    def summarize(
        self,
        max_edges: int = 48,
        max_slot_families: int = 24,
        max_column_ambiguities: int = 24,
    ) -> str:
        lines = [
            f"Tables ({len(self.tables)}): {', '.join(self.tables[:24])}",
            f"Bridge tables: {', '.join(self.bridge_tables) if self.bridge_tables else '(none)'}",
        ]
        if self.edges:
            lines.append("Edges:")
            for edge in self.edges[:max_edges]:
                lines.append(
                    f"- [{edge.relation_type}] {edge.from_table}.{edge.from_column} -> "
                    f"{edge.to_table}.{edge.to_column} (conf={edge.confidence:.2f})"
                )
        else:
            lines.append("Edges: (none)")
        if self.slot_families:
            lines.append("Slot families:")
            for family in self.slot_families[:max_slot_families]:
                lines.append(
                    f"- {family.table}.{family.family_name}: {', '.join(family.columns[:8])}"
                )
        if self.column_ambiguities:
            lines.append("Potential column ownership ambiguities:")
            for item in self.column_ambiguities[:max_column_ambiguities]:
                lines.append(
                    f"- {item.concept}: {', '.join(item.columns[:8])} (conf={item.confidence:.2f})"
                )
        return "\n".join(lines)


def build_schema_graph(
    schema_info: dict[str, Any] | None = None,
    *,
    database_path: str | None = None,
) -> SchemaGraph:
    effective_schema = dict(schema_info or {})
    if not effective_schema.get("tables") and database_path:
        effective_schema.update(get_database_schema(database_path))

    tables_payload = effective_schema.get("tables") or []
    foreign_keys = effective_schema.get("foreign_keys") or []
    tables = [str(table.get("name")) for table in tables_payload if table.get("name")]
    columns_by_table = {
        str(table.get("name")): [
            str(column.get("name")) for column in table.get("columns", []) if column.get("name")
        ]
        for table in tables_payload
        if table.get("name")
    }

    edges: list[GraphEdge] = []
    seen_edges: set[tuple[str, str, str, str, str]] = set()
    for fk in foreign_keys:
        edge = GraphEdge(
            from_table=str(fk.get("from_table", "")),
            from_column=str(fk.get("from_column", "")),
            to_table=str(fk.get("to_table", "")),
            to_column=str(fk.get("to_column", "")),
            relation_type="foreign_key",
            confidence=1.0,
        )
        _append_edge(edges, seen_edges, edge)

    same_name_map: dict[str, list[tuple[str, str]]] = {}
    for table_name, columns in columns_by_table.items():
        for column_name in columns:
            lowered = column_name.lower()
            if not _is_keyish_column(lowered):
                continue
            same_name_map.setdefault(lowered, []).append((table_name, column_name))

    for lowered_name, refs in same_name_map.items():
        if len(refs) < 2 or lowered_name in _GENERIC_KEY_NAMES:
            continue
        for idx, (left_table, left_col) in enumerate(refs):
            for right_table, right_col in refs[idx + 1 :]:
                edge = GraphEdge(
                    from_table=left_table,
                    from_column=left_col,
                    to_table=right_table,
                    to_column=right_col,
                    relation_type="same_name_key",
                    confidence=0.65,
                )
                _append_edge(edges, seen_edges, edge)
                reverse_edge = GraphEdge(
                    from_table=right_table,
                    from_column=right_col,
                    to_table=left_table,
                    to_column=left_col,
                    relation_type="same_name_key",
                    confidence=0.65,
                )
                _append_edge(edges, seen_edges, reverse_edge)

    bridge_tables = [
        table_name
        for table_name, columns in columns_by_table.items()
        if _looks_like_bridge_table(columns)
    ]
    slot_families = _detect_slot_families(columns_by_table)
    column_ambiguities = _detect_column_ambiguities(columns_by_table)

    return SchemaGraph(
        tables=tables,
        columns_by_table=columns_by_table,
        edges=sorted(
            edges,
            key=lambda item: (
                item.relation_type,
                item.from_table,
                item.from_column,
                item.to_table,
                item.to_column,
            ),
        ),
        bridge_tables=sorted(bridge_tables),
        slot_families=slot_families,
        column_ambiguities=column_ambiguities,
    )


def _append_edge(
    edges: list[GraphEdge],
    seen_edges: set[tuple[str, str, str, str, str]],
    edge: GraphEdge,
) -> None:
    key = (
        edge.from_table,
        edge.from_column,
        edge.to_table,
        edge.to_column,
        edge.relation_type,
    )
    if all(key[:4]) and key not in seen_edges:
        seen_edges.add(key)
        edges.append(edge)


def _is_keyish_column(column_name: str) -> bool:
    if column_name in _GENERIC_KEY_NAMES:
        return False
    return column_name.endswith(_KEYISH_SUFFIXES) or column_name in {"cdscode", "uuid"}


def _looks_like_bridge_table(columns: list[str]) -> bool:
    if not columns or len(columns) > 8:
        return False
    lowered = [item.lower() for item in columns]
    keyish = [item for item in lowered if _is_keyish_column(item) or item == "id"]
    non_keyish = [item for item in lowered if item not in keyish]
    return len(keyish) >= 2 and len(non_keyish) <= 2


def _detect_slot_families(columns_by_table: dict[str, list[str]]) -> list[SlotFamily]:
    families: list[SlotFamily] = []
    for table_name, columns in columns_by_table.items():
        grouped: dict[str, list[str]] = {}
        for column_name in columns:
            match = _NUMERIC_SUFFIX_RE.match(column_name)
            if not match:
                continue
            stem = match.group(1).rstrip("_")
            grouped.setdefault(stem, []).append(column_name)
        for stem, family_columns in grouped.items():
            if len(family_columns) < 2:
                continue
            families.append(
                SlotFamily(
                    table=table_name,
                    family_name=stem,
                    columns=sorted(
                        family_columns,
                        key=lambda item: (
                            len(item),
                            item,
                        ),
                    ),
                )
            )
    return sorted(families, key=lambda item: (item.table, item.family_name))


def _detect_column_ambiguities(columns_by_table: dict[str, list[str]]) -> list[ColumnAmbiguity]:
    grouped: dict[str, list[str]] = {}
    for table_name, columns in columns_by_table.items():
        for column_name in columns:
            concept = _normalize_concept(column_name)
            if not concept or _is_ignorable_concept(concept):
                continue
            grouped.setdefault(concept, []).append(f"{table_name}.{column_name}")

    ambiguities: list[ColumnAmbiguity] = []
    for concept, refs in grouped.items():
        unique_tables = {item.split(".", 1)[0] for item in refs}
        if len(unique_tables) < 2:
            continue
        confidence = 0.75 if len(refs) <= 3 else 0.6
        ambiguities.append(
            ColumnAmbiguity(
                concept=concept,
                columns=sorted(refs),
                confidence=confidence,
            )
        )
    return sorted(ambiguities, key=lambda item: (len(item.columns), item.concept), reverse=True)


def _normalize_concept(column_name: str) -> str:
    value = column_name.lower()
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\b(k|12|17|2013|2014)\b", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _is_ignorable_concept(concept: str) -> bool:
    if not concept:
        return True
    tokens = concept.split()
    if len(tokens) == 1 and tokens[0] in {"id", "date", "name", "code", "type"}:
        return True
    return False
