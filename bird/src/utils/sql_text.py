"""SQL text helpers."""

from __future__ import annotations

import re


SQL_BLOCK_PATTERN = re.compile(r"```sql\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)
SQL_START_PATTERN = re.compile(r"(?is)(?:^|\n)\s*(SELECT|WITH)\b")

TRAILING_SECTION_MARKERS = {
    "EXPLANATION",
    "REASONING",
    "NOTES",
    "NOTE",
    "ANSWER",
    "FINAL",
    "THOUGHTS",
}


def extract_sql_from_response(text: str) -> str:
    """Return the last fenced ```sql block from the model response."""

    matches = SQL_BLOCK_PATTERN.findall(text or "")
    if matches:
        return matches[-1].strip()
    return ""


def clean_sql_output(sql_text: str) -> str:
    """Backward-compatible helper that falls back to regex extraction."""

    extracted = extract_sql_from_response(sql_text)
    if extracted:
        return extracted

    raw_text = (sql_text or "").strip()
    if not raw_text:
        return ""

    match = SQL_START_PATTERN.search(raw_text)
    if match:
        candidate = raw_text[match.start(1) :].strip()
    else:
        candidate = raw_text

    lines = candidate.split("\n")
    trimmed_lines = []
    for line in lines:
        line_strip = line.strip()
        if not line_strip:
            trimmed_lines.append(line)
            continue
        if line_strip.startswith("```"):
            break
        marker = line_strip.split()[0].rstrip(":").upper()
        if marker in TRAILING_SECTION_MARKERS:
            break
        trimmed_lines.append(line)

    cleaned = "\n".join(trimmed_lines).strip()
    if SQL_START_PATTERN.search(cleaned):
        return cleaned

    if len(lines) > 1:
        first_line = lines[0].strip()
        if not first_line.upper().startswith(("SELECT", "WITH", "INSERT", "UPDATE", "DELETE")):
            cleaned = "\n".join(lines[1:]).strip()
    return cleaned
