from __future__ import annotations

from langchain_core.tools import tool


@tool
def inspect_schema(db_type: str, connection_string: str, table: str = "") -> str:
    """Inspect a database schema: list tables or describe a specific table's columns.

    Args:
        db_type: Database type: 'postgres' or 'snowflake'.
        connection_string: DSN for Postgres (e.g. postgresql://user:pass@host/db)
                           or Snowflake account identifier (account/user/password/db/schema/wh).
        table: Optional table name to describe. If empty, lists all tables.
    """
    db_type = db_type.lower().strip()
    if db_type == "postgres":
        return _inspect_postgres(connection_string, table)
    elif db_type in ("snowflake", "snow"):
        return _inspect_snowflake(connection_string, table)
    else:
        return f"ERROR: Unknown db_type '{db_type}'. Use 'postgres' or 'snowflake'."


def _inspect_postgres(dsn: str, table: str) -> str:
    try:
        import psycopg2
    except ImportError:
        return "ERROR: psycopg2 not installed. Run: pip install psycopg2-binary"

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()

        if not table:
            cur.execute(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                """
            )
            rows = cur.fetchall()
            conn.close()
            if not rows:
                return "No tables found."
            lines = [f"PostgreSQL tables ({len(rows)}):\n"]
            for schema, tname, ttype in rows:
                lines.append(f"  {schema}.{tname:40s}  [{ttype}]")
            return "\n".join(lines)

        # Describe table
        parts = table.split(".")
        schema_name = parts[0] if len(parts) > 1 else "public"
        table_name = parts[-1]

        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        cols = cur.fetchall()

        # Row count estimate
        try:
            cur.execute(
                "SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s",
                (schema_name, table_name),
            )
            row = cur.fetchone()
            row_estimate = row[0] if row else "unknown"
        except Exception:
            row_estimate = "unknown"

        conn.close()

        if not cols:
            return f"Table '{table}' not found or has no columns."

        lines = [f"PostgreSQL table: {schema_name}.{table_name}  (~{row_estimate} rows)\n"]
        lines.append(f"{'Column':<40} {'Type':<25} {'Nullable':<10} {'Default'}")
        lines.append("-" * 95)
        for col_name, dtype, nullable, default in cols:
            lines.append(
                f"{col_name:<40} {dtype:<25} {nullable:<10} {str(default or '')[:30]}"
            )
        return "\n".join(lines)

    except Exception as exc:
        return f"ERROR connecting to PostgreSQL: {exc}"


def _inspect_snowflake(connection_string: str, table: str) -> str:
    try:
        import snowflake.connector
    except ImportError:
        return "ERROR: snowflake-connector-python not installed."

    # Parse connection_string as account/user/password/database/schema/warehouse
    parts = connection_string.split("/")
    if len(parts) < 3:
        return (
            "ERROR: Snowflake connection_string must be: "
            "account/user/password[/database[/schema[/warehouse]]]"
        )
    account, user, password = parts[0], parts[1], parts[2]
    database = parts[3] if len(parts) > 3 else None
    schema = parts[4] if len(parts) > 4 else None
    warehouse = parts[5] if len(parts) > 5 else None

    try:
        conn_kwargs: dict = {"account": account, "user": user, "password": password}
        if database:
            conn_kwargs["database"] = database
        if schema:
            conn_kwargs["schema"] = schema
        if warehouse:
            conn_kwargs["warehouse"] = warehouse

        conn = snowflake.connector.connect(**conn_kwargs)
        cur = conn.cursor()

        if not table:
            cur.execute("SHOW TABLES")
            rows = cur.fetchall()
            conn.close()
            if not rows:
                return "No tables found."
            lines = [f"Snowflake tables ({len(rows)}):\n"]
            for row in rows[:50]:
                lines.append(f"  {row[1]}.{row[2]}")
            return "\n".join(lines)

        cur.execute(f"DESCRIBE TABLE {table}")
        cols = cur.fetchall()
        conn.close()

        if not cols:
            return f"Table '{table}' not found or has no columns."

        lines = [f"Snowflake table: {table}\n"]
        lines.append(f"{'Column':<40} {'Type':<25} {'Nullable'}")
        lines.append("-" * 75)
        for col in cols:
            col_name = col[0]
            dtype = col[1]
            nullable = col[3] if len(col) > 3 else ""
            lines.append(f"{col_name:<40} {dtype:<25} {nullable}")
        return "\n".join(lines)

    except Exception as exc:
        return f"ERROR connecting to Snowflake: {exc}"
