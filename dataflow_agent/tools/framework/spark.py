from __future__ import annotations

import re
from pathlib import Path

from langchain_core.tools import tool


# Java exception chain pattern
_JAVA_EXCEPTION_RE = re.compile(
    r"((?:java\.|org\.apache\.|scala\.|com\.|py4j\.)\S+(?:Exception|Error)[^\n]*"
    r"(?:\n\s+at [^\n]+)*"
    r"(?:\nCaused by:[^\n]*(?:\n\s+at [^\n]+)*)*)",
    re.MULTILINE,
)

_SPARK_WARN_RE = re.compile(r"^(?:WARN|ERROR|FATAL)\s+\S+:\s+.+", re.MULTILINE)
_OOM_RE = re.compile(r"OutOfMemoryError|GC overhead limit exceeded|java heap space", re.IGNORECASE)
_SHUFFLE_RE = re.compile(r"(?:FetchFailed|shuffle\s+\S+\s+failed|shuffle fetch failed)", re.IGNORECASE)
_STAGE_FAIL_RE = re.compile(r"(?:Stage \d+ .* failed|Job \d+ .* failed)", re.IGNORECASE)
_SKEW_RE = re.compile(r"(?:task\s+is\s+\d+x\s+slower|speculative\s+task|straggler)", re.IGNORECASE)
_EXECUTOR_LOST_RE = re.compile(r"ExecutorLostFailure|executor\s+\d+\s+(lost|dead|failed)", re.IGNORECASE)


@tool
def parse_spark_log(path: str) -> str:
    """Parse a Spark driver log and extract Java exceptions, errors, and performance warnings.

    Identifies OOM errors, shuffle failures, executor losses, and stage failures.

    Args:
        path: Path to the Spark driver log file.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: File not found: {path}"
    if not p.is_file():
        return f"ERROR: Not a file: {path}"

    log_text = p.read_text(encoding="utf-8", errors="replace")
    # Use last 100k chars to focus on recent failures
    if len(log_text) > 100_000:
        log_text = log_text[-100_000:]

    lines: list[str] = [f"Spark log analysis: {p.name}\n"]

    # Java exceptions
    java_exceptions = _JAVA_EXCEPTION_RE.findall(log_text)
    seen: set[str] = set()
    unique_exceptions: list[str] = []
    for exc in java_exceptions:
        key = exc.strip()[:150]
        if key not in seen:
            seen.add(key)
            unique_exceptions.append(exc.strip()[:2000])

    if unique_exceptions:
        lines.append(f"=== JAVA EXCEPTIONS ({len(unique_exceptions)}) ===")
        for i, exc in enumerate(unique_exceptions[:5], 1):
            lines.append(f"\n[Exception {i}]\n{exc}")

    # High-level issues
    issues: list[str] = []

    if _OOM_RE.search(log_text):
        issues.append("CRITICAL: Out-of-Memory error detected — consider increasing executor memory or repartitioning data")

    shuffle_matches = _SHUFFLE_RE.findall(log_text)
    if shuffle_matches:
        issues.append(f"ERROR: Shuffle fetch failures ({len(shuffle_matches)}) — may indicate executor loss or network issues")

    stage_failures = _STAGE_FAIL_RE.findall(log_text)
    if stage_failures:
        issues.append(f"ERROR: Stage/Job failures detected:\n  " + "\n  ".join(set(stage_failures[:10])))

    executor_losses = _EXECUTOR_LOST_RE.findall(log_text)
    if executor_losses:
        issues.append(f"ERROR: Executor failures ({len(executor_losses)}) — check cluster resources and driver logs")

    if _SKEW_RE.search(log_text):
        issues.append("WARN: Data skew detected — some tasks are significantly slower than others")

    # Warn/error lines (deduplicated by prefix)
    warn_lines = _SPARK_WARN_RE.findall(log_text)
    unique_warns: list[str] = []
    warn_keys: set[str] = set()
    for w in warn_lines:
        key = w[:80]
        if key not in warn_keys:
            warn_keys.add(key)
            unique_warns.append(w)

    if unique_warns:
        issues.append(f"\n=== WARN/ERROR LINES (first 20 unique) ===")
        for w in unique_warns[:20]:
            issues.append(f"  {w}")

    if issues:
        lines.append("\n=== ANALYSIS ===")
        lines.extend(issues)
    elif not unique_exceptions:
        lines.append("No significant errors or exceptions found in this log.")

    return "\n".join(lines)
