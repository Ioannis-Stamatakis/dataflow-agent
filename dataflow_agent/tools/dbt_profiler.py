from __future__ import annotations

import re
from pathlib import Path

from langchain_core.tools import tool


@tool
def profile_dbt_project(project_path: str, top_n: int = 10) -> str:
    """Profile all SQL models in a dbt project and rank them by complexity.

    Scans every .sql file under the models/ directory, runs static analysis
    on each, and returns a ranked report of the riskiest models — useful for
    identifying tech debt and optimization targets.

    Args:
        project_path: Path to the dbt project root (must contain a models/ directory).
        top_n: Number of top complex models to highlight (default 10).
    """
    root = Path(project_path)
    if not root.exists():
        return f"ERROR: Path not found: {project_path}"

    models_dir = root / "models"
    if not models_dir.exists():
        # Fallback: treat the path itself as the models dir
        models_dir = root

    sql_files = sorted(models_dir.rglob("*.sql"))
    if not sql_files:
        return f"ERROR: No .sql files found under {models_dir}"

    results: list[dict] = []
    for sql_file in sql_files:
        try:
            sql = sql_file.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            results.append({"file": sql_file.name, "error": str(exc), "score": 0})
            continue

        metrics = _analyze_model(sql)
        metrics["file"] = str(sql_file.relative_to(root))
        metrics["score"] = _complexity_score(metrics)
        results.append(metrics)

    results.sort(key=lambda r: r["score"], reverse=True)

    lines: list[str] = [
        f"dbt Project Profile: {root.name}",
        f"Models scanned: {len(results)}",
        "",
    ]

    # Summary stats
    total_score = sum(r["score"] for r in results)
    avg_score = total_score / len(results) if results else 0
    high_risk = [r for r in results if r["score"] >= 10]
    lines.append(f"Average complexity score : {avg_score:.1f}")
    lines.append(f"High-risk models (≥10)   : {len(high_risk)}")
    lines.append("")

    # Top N complex models
    lines.append(f"=== TOP {min(top_n, len(results))} MOST COMPLEX MODELS ===")
    for rank, m in enumerate(results[:top_n], 1):
        if "error" in m:
            lines.append(f"\n{rank:2}. {m['file']}  [READ ERROR: {m['error']}]")
            continue

        risk = _risk_label(m["score"])
        lines.append(f"\n{rank:2}. {m['file']}")
        lines.append(f"    Score : {m['score']}  [{risk}]")
        lines.append(f"    CTEs  : {m['ctes']}  |  JOINs: {m['joins']}  |  Subqueries: {m['subqueries']}")
        lines.append(f"    Lines : {m['lines']}  |  SELECT *: {m['select_star']}  |  DISTINCT: {m['distinct']}")

        flags: list[str] = []
        if m["select_star"]:
            flags.append("SELECT * detected")
        if m["no_where"] and m["joins"] == 0:
            flags.append("No WHERE clause")
        if m["order_without_limit"]:
            flags.append("ORDER BY without LIMIT")
        if m["leading_wildcard"]:
            flags.append("LIKE with leading wildcard")
        if m["not_in_subquery"]:
            flags.append("NOT IN (subquery) — prefer NOT EXISTS")
        if m["cross_join"]:
            flags.append("CROSS JOIN present")
        if m["joins"] > 5:
            flags.append(f"{m['joins']} JOINs — verify join order")
        if m["ctes"] > 6:
            flags.append(f"{m['ctes']} CTEs — consider splitting into models")

        if flags:
            lines.append("    Flags :")
            for f in flags:
                lines.append(f"      - {f}")

    # Full ranked table
    lines.append("")
    lines.append("=== ALL MODELS RANKED ===")
    lines.append(f"{'Rank':<5} {'Score':<7} {'Risk':<10} {'CTEs':<6} {'JOINs':<7} {'Lines':<7} File")
    lines.append("-" * 80)
    for rank, m in enumerate(results, 1):
        if "error" in m:
            lines.append(f"{rank:<5} {'?':<7} {'ERROR':<10} {'-':<6} {'-':<7} {'-':<7} {m['file']}")
        else:
            lines.append(
                f"{rank:<5} {m['score']:<7} {_risk_label(m['score']):<10} "
                f"{m['ctes']:<6} {m['joins']:<7} {m['lines']:<7} {m['file']}"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Per-model static analysis
# ---------------------------------------------------------------------------

def _analyze_model(sql: str) -> dict:
    sql_upper = sql.upper()
    lines = sql.splitlines()

    # Strip Jinja blocks for cleaner analysis
    clean = re.sub(r"\{\{.*?\}\}", "", sql, flags=re.DOTALL)
    clean = re.sub(r"\{%.*?%\}", "", clean, flags=re.DOTALL)
    clean_upper = clean.upper()

    ctes = len(re.findall(r"\bWITH\b", clean_upper)) + len(re.findall(r",\s*\w+\s+AS\s*\(", clean_upper))
    joins = len(re.findall(r"\bJOIN\b", clean_upper))
    subqueries = len(re.findall(r"\bSELECT\b", clean_upper)) - 1  # subtract the main SELECT
    select_star = bool(re.search(r"SELECT\s+\*", clean_upper))
    distinct = len(re.findall(r"\bDISTINCT\b", clean_upper))
    cross_join = bool(re.search(r"\bCROSS\s+JOIN\b", clean_upper))
    leading_wildcard = bool(re.search(r"LIKE\s+'%\w", sql, re.IGNORECASE))
    not_in_subquery = bool(re.search(r"NOT\s+IN\s*\(\s*SELECT", clean_upper))
    order_without_limit = bool(
        re.search(r"\bORDER\s+BY\b", clean_upper) and not re.search(r"\bLIMIT\b", clean_upper)
    )
    no_where = not bool(re.search(r"\bWHERE\b", clean_upper))

    return {
        "lines": len(lines),
        "ctes": max(0, ctes),
        "joins": joins,
        "subqueries": max(0, subqueries),
        "select_star": select_star,
        "distinct": distinct,
        "cross_join": cross_join,
        "leading_wildcard": leading_wildcard,
        "not_in_subquery": not_in_subquery,
        "order_without_limit": order_without_limit,
        "no_where": no_where,
    }


def _complexity_score(m: dict) -> int:
    score = 0
    score += min(m["ctes"], 10) * 1
    score += min(m["joins"], 10) * 2
    score += min(m["subqueries"], 5) * 2
    score += min(m["lines"] // 50, 10)  # 1 point per 50 lines, capped at 10
    score += 3 if m["select_star"] else 0
    score += m["distinct"] * 1
    score += 4 if m["cross_join"] else 0
    score += 2 if m["leading_wildcard"] else 0
    score += 3 if m["not_in_subquery"] else 0
    score += 1 if m["order_without_limit"] else 0
    return score


def _risk_label(score: int) -> str:
    if score >= 20:
        return "CRITICAL"
    if score >= 10:
        return "HIGH"
    if score >= 5:
        return "MEDIUM"
    return "LOW"
