"""Schema-topology-aware error signal propagation using Personalized PageRank."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Set, Tuple


def build_schema_graph(schema_info: Dict) -> Dict[str, Set[str]]:
    """Build adjacency list from schema info.

    Nodes: table names and "table.column" names
    Edges: table↔column (belong), column↔column (foreign key)
    """
    graph: Dict[str, Set[str]] = defaultdict(set)

    tables = schema_info.get("tables", {})
    for table_name, columns in tables.items():
        for col in columns:
            col_full = f"{table_name}.{col}"
            graph[table_name].add(col_full)
            graph[col_full].add(table_name)

    # Foreign key edges
    for fk in schema_info.get("foreign_keys", []):
        # fk = {"from": "table1.col1", "to": "table2.col2"}
        from_col = fk.get("from", "")
        to_col = fk.get("to", "")
        if from_col and to_col:
            graph[from_col].add(to_col)
            graph[to_col].add(from_col)
            # Also connect the tables
            from_table = from_col.split(".")[0]
            to_table = to_col.split(".")[0]
            graph[from_table].add(to_table)
            graph[to_table].add(from_table)

    return dict(graph)


def personalized_pagerank(
    graph: Dict[str, Set[str]],
    source_nodes: List[str],
    damping: float = 0.85,
    iterations: int = 10,
) -> Dict[str, float]:
    """Compute Personalized PageRank from source_nodes.

    π_e(v) = (1-γ) · 𝟙[v ∈ S]/|S| + γ · Σ_{u∈N(v)} π_e(u)/|N(u)|

    Returns: node → PPR score
    """
    if not source_nodes or not graph:
        return {}

    all_nodes = set(graph.keys())
    for neighbors in graph.values():
        all_nodes.update(neighbors)

    # Initialize
    N = len(all_nodes)
    source_set = set(source_nodes) & all_nodes
    if not source_set:
        return {}

    ppr = {node: 0.0 for node in all_nodes}
    for s in source_set:
        ppr[s] = 1.0 / len(source_set)

    # Iterate
    for _ in range(iterations):
        new_ppr = {node: 0.0 for node in all_nodes}

        for node in all_nodes:
            # Teleport component
            if node in source_set:
                new_ppr[node] += (1 - damping) / len(source_set)

            # Random walk component
            neighbors = graph.get(node, set())
            for neighbor in neighbors:
                out_degree = len(graph.get(neighbor, set()))
                if out_degree > 0:
                    new_ppr[node] += damping * ppr[neighbor] / out_degree

        ppr = new_ppr

    return ppr


def get_propagated_warnings(
    schema_graph: Dict[str, Set[str]],
    error_anchors: List[str],
    target_nodes: List[str],
    threshold: float = 0.01,
) -> List[Tuple[str, float]]:
    """Given error anchors, compute which target nodes receive error signals.

    Returns: [(target_node, signal_strength)] for nodes above threshold.
    """
    ppr = personalized_pagerank(schema_graph, error_anchors)

    results = []
    for node in target_nodes:
        score = ppr.get(node, 0.0)
        if score >= threshold:
            results.append((node, score))

    results.sort(key=lambda x: -x[1])
    return results
