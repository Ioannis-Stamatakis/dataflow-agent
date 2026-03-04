from __future__ import annotations

import glob as _glob
from pathlib import Path

from langchain_core.tools import tool


@tool
def read_file(path: str) -> str:
    """Read any source code or config file and return its contents.

    Args:
        path: Absolute or relative path to the file.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: File not found: {path}"
    if not p.is_file():
        return f"ERROR: Path is not a file: {path}"
    try:
        content = p.read_text(encoding="utf-8", errors="replace")
        max_chars = 30_000
        if len(content) > max_chars:
            content = content[:max_chars] + f"\n\n[...truncated at {max_chars} characters...]"
        return content
    except Exception as exc:
        return f"ERROR reading file: {exc}"


@tool
def list_files(directory: str, pattern: str = "**/*") -> str:
    """List files in a directory matching a glob pattern.

    Args:
        directory: Path to the directory to search.
        pattern: Glob pattern relative to the directory (default: **/*).
    """
    d = Path(directory)
    if not d.exists():
        return f"ERROR: Directory not found: {directory}"
    if not d.is_dir():
        return f"ERROR: Path is not a directory: {directory}"
    try:
        matches = sorted(d.glob(pattern))
        files = [str(p) for p in matches if p.is_file()]
        if not files:
            return f"No files found matching '{pattern}' in {directory}"
        return "\n".join(files)
    except Exception as exc:
        return f"ERROR listing files: {exc}"
