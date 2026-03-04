from __future__ import annotations

import difflib
from pathlib import Path

from langchain_core.tools import tool
from rich.console import Console
from rich.syntax import Syntax

console = Console()


@tool
def write_fix(path: str, original: str, replacement: str) -> str:
    """Apply a fix to a file: show a Rich diff and prompt for confirmation before writing.

    Args:
        path: Path to the file to modify.
        original: The exact text to be replaced (must exist verbatim in the file).
        replacement: The new text to replace it with.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: File not found: {path}"

    try:
        content = p.read_text(encoding="utf-8")
    except Exception as exc:
        return f"ERROR reading file: {exc}"

    if original not in content:
        return (
            f"ERROR: The original text was not found verbatim in {path}.\n"
            "Make sure you are passing the exact text including whitespace and indentation."
        )

    new_content = content.replace(original, replacement, 1)

    # Show diff
    diff_lines = list(
        difflib.unified_diff(
            content.splitlines(keepends=True),
            new_content.splitlines(keepends=True),
            fromfile=f"a/{p.name}",
            tofile=f"b/{p.name}",
            lineterm="",
        )
    )

    if not diff_lines:
        return "No changes — original and replacement are identical."

    diff_text = "".join(diff_lines)
    console.print("\n[bold yellow]Proposed change:[/bold yellow]")
    console.print(
        Syntax(diff_text, "diff", theme="monokai", line_numbers=False)
    )

    try:
        answer = console.input("\n[bold]Apply this fix? [y/N]:[/bold] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return "Fix aborted (no interactive terminal)."

    if answer != "y":
        return "Fix declined by user."

    try:
        p.write_text(new_content, encoding="utf-8")
        return f"Fix applied successfully to {path}."
    except Exception as exc:
        return f"ERROR writing file: {exc}"
