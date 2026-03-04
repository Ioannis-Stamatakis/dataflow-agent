from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml
from langchain_core.tools import tool


@tool
def parse_dbt_manifest(path: str) -> str:
    """Parse a dbt manifest.json or run_results.json and return a structured summary.

    Provides node dependencies, failing tests, and run results.

    Args:
        path: Path to manifest.json or run_results.json.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: File not found: {path}"

    try:
        data: dict[str, Any] = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        return f"ERROR parsing JSON: {exc}"

    filename = p.name.lower()

    # --- run_results.json ---
    if "run_results" in filename or "results" in data:
        return _parse_run_results(data)

    # --- manifest.json ---
    if "nodes" in data or "sources" in data:
        return _parse_manifest(data)

    return f"Unrecognized dbt artifact format in {path}. Keys: {list(data.keys())}"


def _parse_run_results(data: dict) -> str:
    results = data.get("results", [])
    lines = [f"dbt run_results.json — {len(results)} node(s) executed\n"]

    failures = [r for r in results if r.get("status") not in ("success", "pass", "warn")]
    successes = [r for r in results if r.get("status") in ("success", "pass")]
    warnings = [r for r in results if r.get("status") == "warn"]

    lines.append(f"  Succeeded: {len(successes)}")
    lines.append(f"  Warnings:  {len(warnings)}")
    lines.append(f"  Failed:    {len(failures)}\n")

    if failures:
        lines.append("=== FAILED NODES ===")
        for r in failures:
            node_id = r.get("unique_id", "unknown")
            status = r.get("status", "unknown")
            msg = r.get("message", "")
            timing = r.get("execution_time", "")
            lines.append(f"\nNode:    {node_id}")
            lines.append(f"Status:  {status}")
            if timing:
                lines.append(f"Time:    {timing:.2f}s")
            if msg:
                lines.append(f"Message: {msg[:1000]}")

    return "\n".join(lines)


def _parse_manifest(data: dict) -> str:
    nodes: dict = data.get("nodes", {})
    sources: dict = data.get("sources", {})
    exposures: dict = data.get("exposures", {})

    models = {k: v for k, v in nodes.items() if v.get("resource_type") == "model"}
    tests = {k: v for k, v in nodes.items() if v.get("resource_type") == "test"}

    lines = [
        f"dbt manifest.json summary",
        f"  Models:    {len(models)}",
        f"  Tests:     {len(tests)}",
        f"  Sources:   {len(sources)}",
        f"  Exposures: {len(exposures)}\n",
    ]

    if models:
        lines.append("=== MODELS (first 20) ===")
        for name, node in list(models.items())[:20]:
            deps = node.get("depends_on", {}).get("nodes", [])
            dep_str = ", ".join(d.split(".")[-1] for d in deps) if deps else "none"
            lines.append(f"  {name.split('.')[-1]:40s}  depends_on: {dep_str}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# dbt test generator
# ---------------------------------------------------------------------------

@tool
def generate_dbt_tests(
    model_sql_path: str,
    output_path: str = "",
    manifest_path: str = "",
) -> str:
    """Analyse a dbt model SQL file and generate a schema.yml with recommended tests.

    Infers tests from column naming conventions and SQL structure:
    - *_id / id columns  → unique + not_null
    - status/type/state  → accepted_values (values extracted from CASE/IN expressions)
    - foreign keys       → relationships (cross-referenced against manifest models)
    - other columns      → not_null where appropriate

    Args:
        model_sql_path: Path to the dbt model .sql file.
        output_path: Where to write the schema.yml (default: same directory as model).
        manifest_path: Optional path to manifest.json for lineage / FK cross-referencing.
    """
    sql_path = Path(model_sql_path)
    if not sql_path.exists():
        return f"ERROR: Model file not found: {model_sql_path}"

    sql = sql_path.read_text(encoding="utf-8")
    model_name = sql_path.stem

    # Strip dbt Jinja blocks for sqlglot parsing
    clean_sql = _strip_jinja(sql)

    # Extract columns from SELECT
    columns = _extract_columns(clean_sql)
    if not columns:
        return f"ERROR: Could not extract columns from {model_sql_path}. Check the SQL syntax."

    # Extract referenced models from {{ ref(...) }} calls
    ref_models = re.findall(r"{{\s*ref\(['\"](\w+)['\"]\)\s*}}", sql)

    # Load manifest for FK context
    known_models: set[str] = set(ref_models)
    if manifest_path:
        manifest_p = Path(manifest_path)
        if manifest_p.exists():
            try:
                manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
                nodes = manifest.get("nodes", {})
                known_models.update(
                    v["name"] for v in nodes.values()
                    if v.get("resource_type") == "model"
                )
            except Exception:
                pass

    # Extract accepted values hints from SQL
    accepted_values_hints = _extract_accepted_values(sql)

    # Build schema.yml structure
    schema = _build_schema_yml(
        model_name=model_name,
        columns=columns,
        known_models=known_models,
        accepted_values_hints=accepted_values_hints,
    )

    yml_str = yaml.dump(schema, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Determine output path
    out_path = Path(output_path) if output_path else sql_path.parent / "schema.yml"

    # Preview + confirm
    _preview_and_write(yml_str, out_path)

    return f"schema.yml written to {out_path}\n\nContent:\n{yml_str}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_jinja(sql: str) -> str:
    """Remove Jinja2 blocks and replace ref()/source() with table identifiers."""
    sql = re.sub(r"\{\{.*?\}\}", lambda m: _jinja_to_sql(m.group(0)), sql, flags=re.DOTALL)
    sql = re.sub(r"\{%.*?%\}", "", sql, flags=re.DOTALL)
    return sql


def _jinja_to_sql(expr: str) -> str:
    m = re.search(r"ref\(['\"](\w+)['\"]\)", expr)
    if m:
        return m.group(1)
    m = re.search(r"source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)", expr)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    return ""


def _extract_columns(sql: str) -> list[str]:
    """Extract output column names from the final SELECT using sqlglot."""
    try:
        import sqlglot
        import sqlglot.expressions as exp

        statements = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.WARN)
        if not statements:
            return []

        stmt = statements[-1]
        if stmt is None:
            return []

        columns: list[str] = []
        select = stmt if isinstance(stmt, exp.Select) else stmt.find(exp.Select)
        if not select:
            return []

        for sel in select.selects:
            if isinstance(sel, exp.Star):
                columns.extend(_columns_from_cte(stmt))
                break
            alias = sel.alias
            if alias:
                columns.append(alias)
            elif isinstance(sel, exp.Column):
                columns.append(sel.name)

        return columns

    except Exception:
        return _extract_columns_regex(sql)


def _columns_from_cte(stmt) -> list[str]:
    """Walk CTE definitions to find the last one's columns."""
    try:
        import sqlglot.expressions as exp

        ctes = list(stmt.find_all(exp.CTE))
        if not ctes:
            return []

        last_cte = ctes[-1]
        inner = last_cte.find(exp.Select)
        if not inner:
            return []

        cols: list[str] = []
        for sel in inner.selects:
            if isinstance(sel, exp.Star):
                break
            alias = sel.alias
            if alias:
                cols.append(alias)
            elif hasattr(sel, "name"):
                cols.append(sel.name)
        return cols
    except Exception:
        return []


def _extract_columns_regex(sql: str) -> list[str]:
    """Regex fallback: grab aliases and bare column names from SELECT lines."""
    selects = list(re.finditer(r"\bSELECT\b", sql, re.IGNORECASE))
    if not selects:
        return []
    last_pos = selects[-1].end()
    fragment = sql[last_pos:]
    from_match = re.search(r"\bFROM\b", fragment, re.IGNORECASE)
    if from_match:
        fragment = fragment[: from_match.start()]

    columns: list[str] = []
    for line in fragment.split(","):
        line = line.strip().rstrip(",")
        alias_match = re.search(r"\bAS\s+(\w+)\s*$", line, re.IGNORECASE)
        if alias_match:
            columns.append(alias_match.group(1))
            continue
        col_match = re.search(r"(\w+)\s*$", line)
        if col_match:
            columns.append(col_match.group(1))

    return [c for c in columns if c.upper() not in ("SELECT", "FROM", "WHERE", "JOIN")]


def _extract_accepted_values(sql: str) -> dict[str, list[str]]:
    """Extract possible accepted values from CASE WHEN and IN expressions."""
    hints: dict[str, list[str]] = {}

    for m in re.finditer(r"WHEN\s+(\w+)\s*=\s*'([^']+)'", sql, re.IGNORECASE):
        col, val = m.group(1).lower(), m.group(2)
        hints.setdefault(col, [])
        if val not in hints[col]:
            hints[col].append(val)

    for m in re.finditer(r"(\w+)\s+IN\s*\(([^)]+)\)", sql, re.IGNORECASE):
        col = m.group(1).lower()
        vals = re.findall(r"'([^']+)'", m.group(2))
        if vals:
            hints.setdefault(col, [])
            for v in vals:
                if v not in hints[col]:
                    hints[col].append(v)

    return hints


def _build_schema_yml(
    model_name: str,
    columns: list[str],
    known_models: set[str],
    accepted_values_hints: dict[str, list[str]],
) -> dict:
    col_entries: list[dict] = []

    for col in columns:
        col_lower = col.lower()
        entry: dict = {"name": col, "description": "", "tests": []}

        if col_lower in ("id", f"{model_name}_id") or re.match(r"^(surrogate|pk)_", col_lower):
            entry["tests"] = ["unique", "not_null"]

        elif col_lower.endswith("_id"):
            ref_model = _infer_ref_model(col_lower, known_models)
            entry["tests"] = ["not_null"]
            if ref_model:
                entry["tests"].append({
                    "relationships": {
                        "to": f"ref('{ref_model}')",
                        "field": "id" if f"{ref_model}_id" == col_lower else col_lower.replace(f"{ref_model}_", ""),
                    }
                })

        elif any(col_lower == kw or col_lower.endswith(f"_{kw}")
                 for kw in ("status", "type", "state", "category", "tier", "kind", "stage")):
            vals = accepted_values_hints.get(col_lower, [])
            entry["tests"] = ["not_null"]
            if vals:
                entry["tests"].append({"accepted_values": {"values": vals}})

        elif any(col_lower.endswith(sfx) for sfx in ("_at", "_date", "_time", "_ts")):
            entry["tests"] = ["not_null"]

        elif any(kw in col_lower for kw in ("revenue", "amount", "price", "cost", "discount", "quantity")):
            entry["tests"] = ["not_null"]

        elif col_lower == "email" or col_lower.endswith("_email"):
            entry["tests"] = ["unique", "not_null"]

        elif col_lower.startswith(("total_", "count_", "num_", "n_")) or col_lower.endswith("_count"):
            entry["tests"] = ["not_null"]

        else:
            entry["tests"] = []

        col_entries.append(entry)

    return {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": f"Auto-generated schema for {model_name}. Add descriptions.",
                "columns": col_entries,
            }
        ],
    }


def _infer_ref_model(col: str, known_models: set[str]) -> str | None:
    base = col.removeprefix("_id")
    # Try exact and pluralised forms, plus common stg_/int_/fct_ prefixes
    candidates = [base, base + "s", base.rstrip("s")]
    for prefix in ("stg_", "int_", "fct_", "dim_", "raw_"):
        candidates += [prefix + base, prefix + base + "s"]
    for candidate in candidates:
        if candidate in known_models:
            return candidate
    return None


def _preview_and_write(yml_str: str, out_path: Path) -> None:
    try:
        from rich.console import Console
        from rich.syntax import Syntax
        from rich.panel import Panel

        console = Console()
        console.print(
            Panel(
                Syntax(yml_str, "yaml", theme="monokai", line_numbers=True),
                title=f"[bold green]Generated schema.yml → {out_path}[/bold green]",
                border_style="green",
            )
        )
    except Exception:
        print(yml_str)

    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(yml_str, encoding="utf-8")
    except Exception as exc:
        print(f"ERROR writing {out_path}: {exc}")
