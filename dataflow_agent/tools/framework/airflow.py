from __future__ import annotations

import ast
import re
from pathlib import Path

from langchain_core.tools import tool


@tool
def parse_airflow_dag(path: str) -> str:
    """Parse an Airflow DAG file and extract structure: tasks, dependencies, and any syntax errors.

    Args:
        path: Path to the Airflow DAG Python file.
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
            f"SYNTAX ERROR in DAG file {path}:\n"
            f"  Line {exc.lineno}: {exc.msg}\n"
            f"  {exc.text}"
        )

    lines: list[str] = [f"Airflow DAG: {p.name}\n"]

    # Extract DAG ids
    dag_ids = re.findall(r'dag_id\s*=\s*["\']([^"\']+)["\']', source)
    if dag_ids:
        lines.append(f"DAG IDs: {', '.join(dag_ids)}")

    # Extract task operators
    operator_pattern = re.compile(
        r'(\w+)\s*=\s*(\w+Operator|@task|BashOperator|PythonOperator|'
        r'DummyOperator|EmptyOperator|BigQueryOperator|PostgresOperator|'
        r'SparkSubmitOperator|DockerOperator|KubernetesPodOperator)\s*\(',
        re.MULTILINE,
    )
    operators = operator_pattern.findall(source)
    if operators:
        lines.append(f"\nTasks ({len(operators)}):")
        for var_name, op_type in operators[:30]:
            lines.append(f"  {var_name:30s}  [{op_type}]")

    # Extract task_ids
    task_ids = re.findall(r'task_id\s*=\s*["\']([^"\']+)["\']', source)
    if task_ids:
        lines.append(f"\nTask IDs ({len(task_ids)}):")
        for tid in task_ids[:30]:
            lines.append(f"  {tid}")

    # Extract dependency chains (>> operator)
    dep_lines = [line.strip() for line in source.splitlines() if ">>" in line and not line.strip().startswith("#")]
    if dep_lines:
        lines.append(f"\nDependency chains ({len(dep_lines)}):")
        for dl in dep_lines[:20]:
            lines.append(f"  {dl}")

    # Look for common mistakes
    issues: list[str] = []
    if "catchup=True" in source or re.search(r"catchup\s*=\s*True", source):
        issues.append("WARNING: catchup=True — may trigger many historical runs")
    if not re.search(r"catchup\s*=\s*False", source) and not any("catchup=False" in s for s in [source]):
        issues.append("NOTE: catchup not set to False explicitly")
    if re.search(r"retries\s*=\s*0", source):
        issues.append("NOTE: retries=0 — tasks will not retry on failure")
    if "schedule_interval=None" not in source and not re.search(r"schedule\s*=\s*None", source):
        schedule = re.findall(r'schedule(?:_interval)?\s*=\s*["\']([^"\']+)["\']', source)
        if schedule:
            issues.append(f"Schedule: {schedule[0]}")

    if issues:
        lines.append("\nNotes & Warnings:")
        for issue in issues:
            lines.append(f"  {issue}")

    return "\n".join(lines)
