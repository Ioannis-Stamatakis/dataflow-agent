from __future__ import annotations

import re
from langchain_core.tools import tool


@tool
def analyze_sql(query: str, dialect: str = "ansi") -> str:
    """Parse, lint, and provide optimization hints for a SQL query using sqlglot.

    Args:
        query: The SQL query string to analyze.
        dialect: SQL dialect hint: ansi, postgres, snowflake, spark, dbt, bigquery, etc.
    """
    try:
        import sqlglot
        from sqlglot import errors as sg_errors
    except ImportError:
        return "ERROR: sqlglot not installed. Run: pip install sqlglot"

    lines: list[str] = [f"SQL Analysis (dialect: {dialect})\n"]

    # --- Parse ---
    try:
        statements = sqlglot.parse(query, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
    except sg_errors.ParseError as exc:
        return f"PARSE ERROR:\n{exc}"

    if not statements:
        return "No SQL statements found."

    lines.append(f"Statements parsed: {len(statements)}")

    for i, stmt in enumerate(statements, 1):
        if stmt is None:
            continue
        lines.append(f"\n--- Statement {i}: {stmt.__class__.__name__} ---")

        # Pretty-print
        try:
            pretty = stmt.sql(dialect=dialect, pretty=True)
            lines.append("\nFormatted SQL:")
            lines.append(pretty)
        except Exception:
            pass

        # Lint
        try:
            lint_errors = sqlglot.optimizer.qualify.qualify(stmt, dialect=dialect)
        except Exception:
            lint_errors = None

        # Optimization hints
        hints: list[str] = _optimization_hints(stmt, query)
        if hints:
            lines.append("\nOptimization hints:")
            for h in hints:
                lines.append(f"  {h}")

    return "\n".join(lines)


def _optimization_hints(stmt, raw_query: str) -> list[str]:
    hints: list[str] = []
    sql_upper = raw_query.upper()

    # SELECT *
    if "SELECT *" in sql_upper or "SELECT\n*" in sql_upper or "SELECT \n*" in sql_upper:
        hints.append("Avoid SELECT * — specify only needed columns to reduce I/O and enable predicate pushdown")

    # No WHERE clause on large table
    import sqlglot.expressions as exp
    if isinstance(stmt, exp.Select):
        if not stmt.find(exp.Where):
            hints.append("No WHERE clause — consider adding filters to avoid full table scans")

        # Multiple JOINs
        joins = list(stmt.find_all(exp.Join))
        if len(joins) > 3:
            hints.append(f"{len(joins)} JOINs detected — verify join order matches cardinality (smallest table first)")

        # Subqueries
        subqueries = list(stmt.find_all(exp.Subquery))
        if subqueries:
            hints.append(f"{len(subqueries)} subquery/subqueries found — consider CTEs or temp tables for readability and optimization")

        # DISTINCT
        if stmt.find(exp.Distinct):
            hints.append("DISTINCT detected — ensure this is necessary; it forces a sort/aggregate operation")

        # ORDER BY without LIMIT
        if stmt.find(exp.Order) and not stmt.find(exp.Limit):
            hints.append("ORDER BY without LIMIT — sorting the full result set can be expensive")

        # Functions in WHERE (prevents index use)
        where = stmt.find(exp.Where)
        if where:
            funcs_in_where = list(where.find_all(exp.Anonymous)) + list(where.find_all(exp.Func))
            if funcs_in_where:
                hints.append("Function calls in WHERE clause may prevent index usage — consider computed columns or functional indexes")

    # LIKE with leading wildcard
    if re.search(r"LIKE\s+'%\w", raw_query, re.IGNORECASE):
        hints.append("LIKE '%...' with leading wildcard cannot use indexes — consider full-text search")

    # NOT IN with subquery
    if re.search(r"NOT\s+IN\s*\(SELECT", sql_upper):
        hints.append("NOT IN with subquery can be slow — prefer NOT EXISTS or LEFT JOIN ... WHERE IS NULL")

    # CROSS JOIN
    if "CROSS JOIN" in sql_upper:
        hints.append("CROSS JOIN produces a Cartesian product — verify this is intentional")

    if not hints:
        hints.append("No obvious optimization issues found.")

    return hints
