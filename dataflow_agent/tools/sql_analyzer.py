from __future__ import annotations

import re
from langchain_core.tools import tool


@tool
def explain_query(
    query: str,
    db_type: str,
    connection_string: str,
    dialect: str = "",
) -> str:
    """Run EXPLAIN ANALYZE on a SQL query and return a plain-English interpretation.

    Executes the query plan against a live database, then returns the raw plan
    plus structured observations about sequential scans, missing indexes,
    expensive sorts, bad join strategies, and row estimate errors.

    Args:
        query: The SELECT query to explain (must be a read-only SELECT).
        db_type: Database type: 'postgres' or 'snowflake'.
        connection_string: DSN for Postgres (postgresql://user:pass@host/db)
                           or Snowflake account string (account/user/password/db/schema/wh).
        dialect: Optional sqlglot dialect hint for query formatting.
    """
    db_type = db_type.lower().strip()
    if db_type == "postgres":
        return _explain_postgres(query, connection_string, dialect)
    elif db_type in ("snowflake", "snow"):
        return _explain_snowflake(query, connection_string)
    else:
        return f"ERROR: Unknown db_type '{db_type}'. Use 'postgres' or 'snowflake'."


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

def _explain_postgres(query: str, dsn: str, dialect: str) -> str:
    try:
        import psycopg2
    except ImportError:
        return "ERROR: psycopg2 not installed. Run: pip install psycopg2-binary"

    # Safety: only allow SELECT statements
    stripped = query.strip().lstrip("(").upper()
    if not stripped.startswith(("SELECT", "WITH", "TABLE")):
        return "ERROR: explain_query only accepts SELECT / WITH / TABLE statements for safety."

    try:
        conn = psycopg2.connect(dsn)
        conn.set_session(readonly=True)
        cur = conn.cursor()
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}")
        rows = cur.fetchall()
        conn.close()
    except Exception as exc:
        return f"ERROR running EXPLAIN ANALYZE: {exc}"

    plan_lines = [row[0] for row in rows]
    plan_text = "\n".join(plan_lines)

    observations = _analyze_postgres_plan(plan_text)

    sections = [
        "=== QUERY PLAN ===",
        plan_text,
        "",
        "=== OBSERVATIONS ===",
    ]
    sections.extend(f"  {o}" for o in observations)
    return "\n".join(sections)


def _analyze_postgres_plan(plan: str) -> list[str]:
    obs: list[str] = []

    # Seq scans
    seq_scans = re.findall(r"Seq Scan on (\S+)", plan)
    if seq_scans:
        tables = ", ".join(dict.fromkeys(seq_scans))
        obs.append(
            f"SEQUENTIAL SCAN on: {tables} — consider adding an index on the filter columns"
        )

    # Nested loop with large row estimates
    if re.search(r"Nested Loop.*rows=\d{5,}", plan, re.DOTALL):
        obs.append(
            "NESTED LOOP join on large row sets detected — a Hash Join or Merge Join may be faster"
        )

    # Hash join spill to disk
    if re.search(r"Batches: [2-9]\d*|Batches: \d{2,}", plan):
        obs.append(
            "HASH JOIN spilling to disk (multiple batches) — increase work_mem to keep it in memory"
        )

    # Sort spill
    if re.search(r"Sort Method: external", plan, re.IGNORECASE):
        obs.append(
            "SORT spilling to disk — increase work_mem or add an index to avoid the sort"
        )

    # Row estimate errors (actual >> estimated)
    misestimates = re.findall(
        r"rows=(\d+).*?actual.*?rows=(\d+)", plan
    )
    for est, actual in misestimates:
        est_n, actual_n = int(est), int(actual)
        if actual_n > 0 and est_n > 0:
            ratio = max(est_n, actual_n) / min(est_n, actual_n)
            if ratio >= 100:
                obs.append(
                    f"ROW ESTIMATE ERROR: planner estimated {est_n} rows but got {actual_n} "
                    f"({ratio:.0f}x off) — run ANALYZE on the table to update statistics"
                )
                break  # report once

    # Index scans (good — just note them)
    index_scans = re.findall(r"Index(?:\s+Only)?\s+Scan(?:\s+Backward)?\s+using\s+(\S+)", plan)
    if index_scans:
        obs.append(f"INDEX SCAN used: {', '.join(dict.fromkeys(index_scans))} ✓")

    # Execution time
    exec_time = re.search(r"Execution Time:\s+([\d.]+)\s+ms", plan)
    if exec_time:
        ms = float(exec_time.group(1))
        label = "✓ fast" if ms < 100 else ("moderate" if ms < 1000 else "SLOW — consider optimization")
        obs.append(f"EXECUTION TIME: {ms:.2f} ms — {label}")

    # Planning time
    plan_time = re.search(r"Planning Time:\s+([\d.]+)\s+ms", plan)
    if plan_time:
        ms = float(plan_time.group(1))
        if ms > 500:
            obs.append(
                f"PLANNING TIME: {ms:.2f} ms is high — complex query with many joins/subqueries"
            )

    if not obs:
        obs.append("No significant issues found in the query plan.")

    return obs


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------

def _explain_snowflake(query: str, connection_string: str) -> str:
    try:
        import snowflake.connector
    except ImportError:
        return "ERROR: snowflake-connector-python not installed."

    stripped = query.strip().lstrip("(").upper()
    if not stripped.startswith(("SELECT", "WITH", "TABLE")):
        return "ERROR: explain_query only accepts SELECT / WITH / TABLE statements for safety."

    parts = connection_string.split("/")
    if len(parts) < 3:
        return "ERROR: Snowflake connection_string must be: account/user/password[/database[/schema[/warehouse]]]"

    account, user, password = parts[0], parts[1], parts[2]
    conn_kwargs: dict = {"account": account, "user": user, "password": password}
    if len(parts) > 3:
        conn_kwargs["database"] = parts[3]
    if len(parts) > 4:
        conn_kwargs["schema"] = parts[4]
    if len(parts) > 5:
        conn_kwargs["warehouse"] = parts[5]

    try:
        conn = snowflake.connector.connect(**conn_kwargs)
        cur = conn.cursor()
        cur.execute(f"EXPLAIN {query}")
        rows = cur.fetchall()
        conn.close()
    except Exception as exc:
        return f"ERROR running EXPLAIN on Snowflake: {exc}"

    plan_lines = [str(row) for row in rows]
    plan_text = "\n".join(plan_lines)

    observations = _analyze_snowflake_plan(plan_text)

    sections = [
        "=== SNOWFLAKE QUERY PLAN ===",
        plan_text,
        "",
        "=== OBSERVATIONS ===",
    ]
    sections.extend(f"  {o}" for o in observations)
    return "\n".join(sections)


def _analyze_snowflake_plan(plan: str) -> list[str]:
    obs: list[str] = []
    plan_upper = plan.upper()

    if "TABLE SCAN" in plan_upper:
        obs.append(
            "FULL TABLE SCAN detected — ensure the query filters on clustering keys or partition columns"
        )
    if "CARTESIAN" in plan_upper:
        obs.append("CARTESIAN PRODUCT in plan — check for missing JOIN conditions")
    if "SORT" in plan_upper:
        obs.append("SORT operation present — large sorts may spill to remote storage on Snowflake")
    if "UNION" in plan_upper:
        obs.append("UNION detected — use UNION ALL if duplicates don't need to be removed (avoids dedup pass)")

    if not obs:
        obs.append("No significant issues found in the Snowflake query plan.")

    return obs


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
