from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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
