from __future__ import annotations

import collections
import re
from typing import Any

from src.structured_generation.schema_graph import build_schema_graph


def search_anchor_swap_hints(
    *,
    schema_info: dict[str, Any] | None,
    database_path: str,
    task_spec: dict[str, Any],
    query_spec: dict[str, Any],
    max_hints: int = 8,
) -> list[dict[str, Any]]:
    graph = build_schema_graph(schema_info or {}, database_path=database_path)
    current_tables = [
        str(table).lower()
        for table in (query_spec.get("base_tables") or [])
        if str(table).strip()
    ]
    current_tables += [
        str(join.get("table") or "").lower()
        for join in (query_spec.get("joins") or [])
        if str(join.get("table") or "").strip()
    ]
    current_tables = list(dict.fromkeys(current_tables))
    if not current_tables:
        return []

    target_tokens = _collect_target_tokens(task_spec)
    if not target_tokens:
        return []

    candidate_tables = []
    for table in graph.tables:
        table_l = str(table).lower()
        score = _table_match_score(table_l, target_tokens, task_spec)
        if score <= 0 or table_l in current_tables:
            continue
        candidate_tables.append((table_l, score))

    hints: list[dict[str, Any]] = []
    for target_table, table_score in sorted(candidate_tables, key=lambda item: (-item[1], item[0])):
        for current_table in current_tables:
            path = _find_path(graph, current_table, target_table, max_depth=2)
            if not path:
                continue
            hints.append(
                {
                    "current_table": current_table,
                    "target_table": target_table,
                    "path": path,
                    "score": table_score + max(0, 30 - 5 * len(path)),
                    "trigger": "entity_or_field_match",
                }
            )

    hints.sort(key=lambda item: (-int(item["score"]), item["target_table"], item["current_table"]))
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in hints:
        key = (item["current_table"], item["target_table"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= max_hints:
            break
    return deduped


def _collect_target_tokens(task_spec: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for raw in [task_spec.get("target_entity_hint", "")] + list(task_spec.get("requested_fields") or []):
        text = str(raw or "").lower()
        for token in re.split(r"[^a-z0-9_]+", text):
            token = token.strip()
            if not token or len(token) <= 2:
                continue
            tokens.add(token)
            if token.endswith("s") and len(token) > 3:
                tokens.add(token[:-1])
    return tokens


def _table_match_score(table_name: str, target_tokens: set[str], task_spec: dict[str, Any]) -> int:
    score = 0
    table_tokens = _table_tokens(table_name)
    if target_tokens & table_tokens:
        score += 60
    requested_fields = {str(x).lower() for x in (task_spec.get("requested_fields") or [])}
    if requested_fields & table_tokens:
        score += 10
    return score


def _table_tokens(table_name: str) -> set[str]:
    tokens = set()
    for part in str(table_name or "").lower().split("_"):
        if not part:
            continue
        tokens.add(part)
        if part.endswith("s") and len(part) > 3:
            tokens.add(part[:-1])
    return tokens


def _find_path(graph, start_table: str, target_table: str, max_depth: int = 2) -> list[dict[str, Any]]:
    adjacency = collections.defaultdict(list)
    for edge in graph.edges:
        adjacency[str(edge.from_table).lower()].append(
            {
                "to_table": str(edge.to_table).lower(),
                "from_table": str(edge.from_table).lower(),
                "from_column": str(edge.from_column),
                "to_column": str(edge.to_column),
                "relation_type": str(edge.relation_type),
                "confidence": float(edge.confidence),
            }
        )
    queue = collections.deque([(start_table.lower(), [])])
    seen = {start_table.lower()}
    while queue:
        node, path = queue.popleft()
        if len(path) >= max_depth:
            continue
        for edge in adjacency[node]:
            nxt = edge["to_table"]
            if nxt in seen:
                continue
            new_path = path + [edge]
            if nxt == target_table.lower():
                return new_path
            seen.add(nxt)
            queue.append((nxt, new_path))
    return []
