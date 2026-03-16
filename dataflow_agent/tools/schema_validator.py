from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from langchain_core.tools import tool


@tool
def validate_dbt_coverage(
    project_path: str,
    schema_path: str = "",
    model_name: str = "",
) -> str:
    """Check dbt schema.yml test coverage across models without a database connection.

    Reports:
    - Models with no schema.yml entry
    - Models with partial test coverage (some columns untested)
    - Models with full test coverage

    Args:
        project_path: Path to the dbt project root (must contain a models/ directory).
        schema_path: Explicit path to a single schema.yml file (default: auto-discover all schema*.yml).
        model_name: Specific model name to check (default: check all models).
    """
    root = Path(project_path)
    if not root.exists():
        return f"ERROR: Project path not found: {project_path}"

    models_dir = root / "models"
    if not models_dir.exists():
        return f"ERROR: No models/ directory found under {project_path}"

    sql_models = _collect_sql_models(models_dir, model_name)
    if not sql_models:
        msg = f"No .sql files found under {models_dir}"
        if model_name:
            msg += f" matching model '{model_name}'"
        return f"ERROR: {msg}"

    schema_files = _find_schema_files(root, schema_path)
    schema_entries = _collect_schema_entries(schema_files, model_name)

    models_without_schema: list[str] = sorted(sql_models - set(schema_entries.keys()))
    coverage: dict[str, dict[str, list[str]]] = {}
    for name, node in schema_entries.items():
        if name not in sql_models:
            continue
        tested, untested = _analyze_column_coverage(node)
        coverage[name] = {"tested": tested, "untested": untested}

    return _format_coverage_report(
        total_sql=len(sql_models),
        total_schema=len(schema_entries),
        models_without_schema=models_without_schema,
        coverage=coverage,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _collect_sql_models(models_dir: Path, model_name: str) -> set[str]:
    """Return the set of model names (without .sql) found under models/."""
    names = {p.stem for p in models_dir.rglob("*.sql")}
    if model_name:
        names = {n for n in names if n == model_name}
    return names


def _find_schema_files(root: Path, schema_path: str) -> list[Path]:
    """Return schema YAML files to inspect."""
    if schema_path:
        p = Path(schema_path)
        return [p] if p.exists() else []
    models_dir = root / "models"
    files = list(models_dir.rglob("schema*.yml")) + list(models_dir.rglob("schema*.yaml"))
    # Also accept _schema.yml and models.yml conventions
    files += list(models_dir.rglob("_schema*.yml")) + list(models_dir.rglob("models.yml"))
    return list({f.resolve(): f for f in files}.values())  # deduplicate


def _collect_schema_entries(
    schema_files: list[Path],
    model_name: str,
) -> dict[str, dict[str, Any]]:
    """Return {model_name: schema_node} from all schema files."""
    entries: dict[str, dict[str, Any]] = {}
    for path in schema_files:
        try:
            data: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        for model in data.get("models", []):
            name = model.get("name", "")
            if not name:
                continue
            if model_name and name != model_name:
                continue
            entries[name] = model
    return entries


def _analyze_column_coverage(model_node: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Return (tested_columns, untested_columns) for a schema model node."""
    tested: list[str] = []
    untested: list[str] = []
    for col in model_node.get("columns", []):
        col_name = col.get("name", "")
        if not col_name:
            continue
        if col.get("tests"):
            tested.append(col_name)
        else:
            untested.append(col_name)
    return tested, untested


def _format_coverage_report(
    total_sql: int,
    total_schema: int,
    models_without_schema: list[str],
    coverage: dict[str, dict[str, list[str]]],
) -> str:
    fully_tested = [m for m, c in coverage.items() if not c["untested"]]
    partially_tested = [m for m, c in coverage.items() if c["untested"]]

    total_cols = sum(len(c["tested"]) + len(c["untested"]) for c in coverage.values())
    tested_cols = sum(len(c["tested"]) for c in coverage.values())

    lines: list[str] = [
        "dbt Test Coverage Report",
        f"Models scanned: {total_sql}  |  Schema entries found: {total_schema}",
        "",
    ]

    if models_without_schema:
        lines.append(f"=== MODELS WITHOUT SCHEMA ENTRY ({len(models_without_schema)}) ===")
        for m in models_without_schema:
            lines.append(f"  - {m}")
        lines.append("")

    if partially_tested:
        lines.append(f"=== MODELS WITH PARTIAL TEST COVERAGE ({len(partially_tested)}) ===")
        for m in partially_tested:
            c = coverage[m]
            total_m = len(c["tested"]) + len(c["untested"])
            lines.append(f"Model: {m}  ({len(c['tested'])}/{total_m} columns tested)")
            lines.append("  [UNTESTED COLUMNS]")
            for col in c["untested"]:
                lines.append(f"    - {col}")
            lines.append("")

    if fully_tested:
        lines.append(f"=== MODELS WITH FULL TEST COVERAGE ({len(fully_tested)}) ===")
        for m in fully_tested:
            lines.append(f"  - {m}")
        lines.append("")

    schema_pct = round(total_schema / total_sql * 100) if total_sql else 0
    col_pct = round(tested_cols / total_cols * 100) if total_cols else 0

    lines.append("=== COVERAGE SUMMARY ===")
    lines.append(f"  Schema coverage : {total_schema}/{total_sql} models ({schema_pct}%)")
    lines.append(f"  Column coverage : {tested_cols}/{total_cols} columns ({col_pct}%)")

    return "\n".join(lines)
