from __future__ import annotations

import re
from typing import Any

from langchain_core.tools import tool


# ---------------------------------------------------------------------------
# Framework-specific patterns
# ---------------------------------------------------------------------------

_DBT_ERROR_PATTERNS = [
    re.compile(r"Compilation Error.*", re.DOTALL | re.MULTILINE),
    re.compile(r"Database Error.*", re.DOTALL | re.MULTILINE),
    re.compile(r"Runtime Error.*", re.DOTALL | re.MULTILINE),
    re.compile(r"(ERROR|WARN)\s+.*", re.MULTILINE),
    re.compile(r"Unhandled exception.*", re.DOTALL | re.MULTILINE),
    re.compile(r"(?:node \S+ had an error|does not compile)", re.MULTILINE | re.IGNORECASE),
]

_AIRFLOW_ERROR_PATTERNS = [
    re.compile(r"Traceback \(most recent call last\).*?(?=\n\S|\Z)", re.DOTALL),
    re.compile(r"(ERROR|CRITICAL)\s+-\s+.*", re.MULTILINE),
    re.compile(r"Task\s+\S+\s+failed.*", re.MULTILINE | re.IGNORECASE),
    re.compile(r"(?:DagBag|Import)Error:.*", re.MULTILINE),
    re.compile(r"AirflowException:.*", re.MULTILINE),
    re.compile(r"(?:slot|pool)\s+\S+\s+is full", re.MULTILINE | re.IGNORECASE),
]

_PREFECT_ERROR_PATTERNS = [
    re.compile(r"Traceback \(most recent call last\).*?(?=\n\S|\Z)", re.DOTALL),
    re.compile(r"(ERROR|CRITICAL)\s+.*", re.MULTILINE),
    re.compile(r"Flow run .* crashed.*", re.MULTILINE | re.IGNORECASE),
    re.compile(r"Task run .* failed.*", re.MULTILINE | re.IGNORECASE),
    re.compile(r"CrashException:.*", re.MULTILINE),
]

_SPARK_ERROR_PATTERNS = [
    re.compile(
        r"(?:java\.lang\.|org\.apache\.|py4j\.)\S+Exception.*?(?=\n\s*at |\Z)",
        re.DOTALL,
    ),
    re.compile(r"at [\w\.$]+ \([\w\.$]+\.(?:java|scala):\d+\)", re.MULTILINE),
    re.compile(r"Caused by:.*", re.MULTILINE),
    re.compile(r"(ERROR|WARN)\s+.*", re.MULTILINE),
    re.compile(r"(?:AnalysisException|SparkException|PythonException):.*", re.MULTILINE),
    re.compile(r"WARN\s+TaskSetManager.*", re.MULTILINE),
    re.compile(r"Job \d+ failed.*", re.MULTILINE | re.IGNORECASE),
    re.compile(r"Stage \d+ failed.*", re.MULTILINE | re.IGNORECASE),
    re.compile(r"OutOfMemoryError.*", re.MULTILINE | re.IGNORECASE),
    re.compile(r"GC overhead limit exceeded", re.MULTILINE | re.IGNORECASE),
]

_GENERIC_ERROR_PATTERNS = [
    re.compile(r"Traceback \(most recent call last\).*?(?=\n\S|\Z)", re.DOTALL),
    re.compile(r"(ERROR|CRITICAL|FATAL)\s+.*", re.MULTILINE),
    re.compile(r"Exception:.*", re.MULTILINE),
]

_FRAMEWORK_MAP: dict[str, list[re.Pattern]] = {
    "dbt": _DBT_ERROR_PATTERNS,
    "airflow": _AIRFLOW_ERROR_PATTERNS,
    "prefect": _PREFECT_ERROR_PATTERNS,
    "spark": _SPARK_ERROR_PATTERNS,
}


def _extract(log_text: str, patterns: list[re.Pattern]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for pat in patterns:
        for match in pat.finditer(log_text):
            snippet = match.group(0).strip()
            # Deduplicate and cap length
            key = snippet[:200]
            if key not in seen:
                seen.add(key)
                results.append(snippet[:2000])
    return results


@tool
def extract_errors(log_text: str, framework: str = "generic") -> str:
    """Extract structured error information from raw log text.

    Args:
        log_text: The raw log content as a string.
        framework: The pipeline framework: dbt, airflow, prefect, spark, or generic.

    Returns:
        A formatted string listing extracted errors and warnings.
    """
    patterns = _FRAMEWORK_MAP.get(framework.lower(), _GENERIC_ERROR_PATTERNS)
    errors = _extract(log_text, patterns)

    if not errors:
        return "No errors or warnings detected in the provided log text."

    lines = [f"Found {len(errors)} error/warning snippet(s):\n"]
    for i, err in enumerate(errors, 1):
        lines.append(f"--- Snippet {i} ---\n{err}\n")
    return "\n".join(lines)
