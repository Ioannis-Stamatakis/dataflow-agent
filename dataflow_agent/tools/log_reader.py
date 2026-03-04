from __future__ import annotations

from pathlib import Path

from langchain_core.tools import tool


@tool
def read_log(path: str) -> str:
    """Read the contents of a log file and return them as a string.

    Args:
        path: Absolute or relative path to the log file.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: Log file not found: {path}"
    if not p.is_file():
        return f"ERROR: Path is not a file: {path}"
    try:
        content = p.read_text(encoding="utf-8", errors="replace")
        # Truncate very large logs to avoid context overflow
        max_chars = 50_000
        if len(content) > max_chars:
            content = content[-max_chars:]
            content = f"[...truncated to last {max_chars} characters...]\n\n" + content
        return content
    except Exception as exc:
        return f"ERROR reading log file: {exc}"
