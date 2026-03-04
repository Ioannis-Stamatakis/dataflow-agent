from __future__ import annotations

import ast
import re
from pathlib import Path

from langchain_core.tools import tool


@tool
def parse_prefect_flow(path: str) -> str:
    """Parse a Prefect flow file and extract flows, tasks, dependencies, and potential issues.

    Args:
        path: Path to the Prefect flow Python file.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: File not found: {path}"
    if not p.is_file():
        return f"ERROR: Not a file: {path}"

    source = p.read_text(encoding="utf-8", errors="replace")

    # Syntax check
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return (
            f"SYNTAX ERROR in Prefect flow file {path}:\n"
            f"  Line {exc.lineno}: {exc.msg}\n"
            f"  {exc.text}"
        )

    lines: list[str] = [f"Prefect flow: {p.name}\n"]

    # Find @flow decorated functions
    flow_pattern = re.compile(
        r"@(?:flow|prefect\.flow)\s*(?:\([^)]*\))?\s*\ndef\s+(\w+)\s*\(",
        re.MULTILINE,
    )
    flows = flow_pattern.findall(source)
    if flows:
        lines.append(f"Flows ({len(flows)}): {', '.join(flows)}")

    # Find @task decorated functions
    task_pattern = re.compile(
        r"@(?:task|prefect\.task)\s*(?:\([^)]*\))?\s*\ndef\s+(\w+)\s*\(",
        re.MULTILINE,
    )
    tasks = task_pattern.findall(source)
    if tasks:
        lines.append(f"\nTasks ({len(tasks)}):")
        for t in tasks:
            lines.append(f"  {t}")

    # Find retries config
    retries = re.findall(r"retries\s*=\s*(\d+)", source)
    if retries:
        lines.append(f"\nRetry configs found: {retries}")

    # Find deployments / schedules
    schedules = re.findall(r'(?:cron|interval|rrule)\s*=\s*["\']?([^"\',\)]+)', source)
    if schedules:
        lines.append(f"\nSchedules: {schedules}")

    # Check for common issues
    issues: list[str] = []
    if "submit" in source and ".result()" not in source:
        issues.append("NOTE: .submit() used without .result() — results may not be awaited")
    if re.search(r"@flow.*\n.*def\s+\w+.*\n(?:.*\n)*?.*@flow", source, re.MULTILINE):
        issues.append("WARNING: Nested @flow decorators detected")
    if "raise_on_failure" in source:
        issues.append("NOTE: raise_on_failure is set — failures will propagate as exceptions")

    if issues:
        lines.append("\nNotes & Warnings:")
        for issue in issues:
            lines.append(f"  {issue}")

    return "\n".join(lines)
