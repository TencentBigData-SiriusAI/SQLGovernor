"""Entry point exposing the compiled SQL agent graph for LangGraph Studio."""

from src.core.graph import compile_sql_agent_graph

# Expose the compiled graph for LangGraph Studio (`langgraph dev`).
graph = compile_sql_agent_graph()

# Alias for convenience.
app = graph

__all__ = ["graph", "app"]
