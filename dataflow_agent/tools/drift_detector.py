from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from langchain_core.tools import tool


@tool
def detect_schema_drift(
    schema_yml_path: str,
    db_type: str,
    connection_string: str,
    model_name: str = "",
) -> str:
    """Compare a dbt schema.yml definition against the live database schema and report drift.

    Detects:
    - Columns defined in schema.yml but missing from the live table
    - Columns in the live table not documented in schema.yml
    - Data type mismatches between schema.yml and the live table

    Args:
        schema_yml_path: Path to the dbt schema.yml file.
        db_type: Database type: 'postgres' or 'snowflake'.
        connection_string: DSN for Postgres or account/user/pass/db/schema/wh for Snowflake.
        model_name: Specific model name to check (default: check all models in file).
    """
    p = Path(schema_yml_path)
    if not p.exists():
        return f"ERROR: schema.yml not found: {schema_yml_path}"

    try:
        schema_data: dict[str, Any] = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as exc:
        return f"ERROR parsing schema.yml: {exc}"

    models = schema_data.get("models", [])
    if not models:
        return f"ERROR: No models found in {schema_yml_path}"

    if model_name:
        models = [m for m in models if m.get("name") == model_name]
        if not models:
            return f"ERROR: Model '{model_name}' not found in {schema_yml_path}"

    results: list[str] = []

    for model in models:
        name = model.get("name", "unknown")
        defined_columns = {
            col["name"].lower(): col
            for col in model.get("columns", [])
            if "name" in col
        }

        live_columns = _fetch_live_columns(db_type, connection_string, name)
        if isinstance(live_columns, str):
            # It's an error message
            results.append(f"Model: {name}\n  ERROR: {live_columns}\n")
            continue

        drift = _compare_columns(defined_columns, live_columns)
        results.append(_format_model_drift(name, drift))

    return "\n".join(results) if results else "No drift detected."


def _fetch_live_columns(
    db_type: str, connection_string: str, table: str
) -> dict[str, str] | str:
    """Return {col_name_lower: data_type} from the live DB, or an error string."""
    db_type = db_type.lower().strip()
    if db_type == "postgres":
        return _fetch_postgres_columns(connection_string, table)
    elif db_type in ("snowflake", "snow"):
        return _fetch_snowflake_columns(connection_string, table)
    else:
        return f"Unknown db_type '{db_type}'. Use 'postgres' or 'snowflake'."


def _fetch_postgres_columns(dsn: str, table: str) -> dict[str, str] | str:
    try:
        import psycopg2
    except ImportError:
        return "psycopg2 not installed. Run: pip install psycopg2-binary"

    parts = table.split(".")
    schema_name = parts[0] if len(parts) > 1 else "public"
    table_name = parts[-1]

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception as exc:
        return f"PostgreSQL connection error: {exc}"

    if not rows:
        return f"Table '{table}' not found in database (schema={schema_name})."

    return {col_name.lower(): dtype for col_name, dtype in rows}


def _fetch_snowflake_columns(connection_string: str, table: str) -> dict[str, str] | str:
    try:
        import snowflake.connector
    except ImportError:
        return "snowflake-connector-python not installed."

    parts = connection_string.split("/")
    if len(parts) < 3:
        return "Snowflake connection_string must be: account/user/password[/database[/schema[/warehouse]]]"

    account, user, password = parts[0], parts[1], parts[2]
    database = parts[3] if len(parts) > 3 else None
    schema = parts[4] if len(parts) > 4 else None
    warehouse = parts[5] if len(parts) > 5 else None

    conn_kwargs: dict = {"account": account, "user": user, "password": password}
    if database:
        conn_kwargs["database"] = database
    if schema:
        conn_kwargs["schema"] = schema
    if warehouse:
        conn_kwargs["warehouse"] = warehouse

    try:
        conn = snowflake.connector.connect(**conn_kwargs)
        cur = conn.cursor()
        cur.execute(f"DESCRIBE TABLE {table}")
        rows = cur.fetchall()
        conn.close()
    except Exception as exc:
        return f"Snowflake connection error: {exc}"

    if not rows:
        return f"Table '{table}' not found in Snowflake."

    return {row[0].lower(): row[1] for row in rows}


def _compare_columns(
    defined: dict[str, Any],
    live: dict[str, str],
) -> dict[str, list[str]]:
    """Return drift categories: missing_in_db, undocumented, type_mismatch."""
    missing_in_db: list[str] = []
    undocumented: list[str] = []
    type_mismatch: list[str] = []

    for col_name in defined:
        if col_name not in live:
            missing_in_db.append(col_name)

    for col_name, live_type in live.items():
        if col_name not in defined:
            undocumented.append(f"{col_name}  (db_type: {live_type})")

    # Type mismatch: only when schema.yml explicitly declares data_type
    for col_name, col_def in defined.items():
        if col_name not in live:
            continue
        declared_type = col_def.get("data_type", "").lower().strip()
        if not declared_type:
            continue
        live_type = live[col_name].lower().strip()
        # Normalise common aliases
        if declared_type != live_type and not _types_compatible(declared_type, live_type):
            type_mismatch.append(
                f"{col_name}  schema.yml={declared_type}, db={live_type}"
            )

    return {
        "missing_in_db": missing_in_db,
        "undocumented": undocumented,
        "type_mismatch": type_mismatch,
    }


def _types_compatible(t1: str, t2: str) -> bool:
    """Return True if two type strings are effectively equivalent aliases."""
    aliases = [
        {"integer", "int", "int4", "int8", "bigint", "smallint", "int2"},
        {"character varying", "varchar", "text", "string"},
        {"double precision", "float", "float8", "numeric", "decimal", "real", "float4"},
        {"boolean", "bool"},
        {"timestamp without time zone", "timestamp", "datetime"},
        {"timestamp with time zone", "timestamptz"},
    ]
    for group in aliases:
        if t1 in group and t2 in group:
            return True
    return False


def _format_model_drift(name: str, drift: dict[str, list[str]]) -> str:
    missing = drift["missing_in_db"]
    undoc = drift["undocumented"]
    mismatch = drift["type_mismatch"]

    total = len(missing) + len(undoc) + len(mismatch)
    lines = [f"Model: {name}"]

    if total == 0:
        lines.append("  No drift detected — schema.yml matches live table.\n")
        return "\n".join(lines)

    lines.append(f"  {total} issue(s) found:\n")

    if missing:
        lines.append(f"  [MISSING IN DB]  ({len(missing)} column(s))")
        for col in missing:
            lines.append(f"    - {col}")

    if undoc:
        lines.append(f"  [UNDOCUMENTED]   ({len(undoc)} column(s))")
        for col in undoc:
            lines.append(f"    - {col}")

    if mismatch:
        lines.append(f"  [TYPE MISMATCH]  ({len(mismatch)} column(s))")
        for col in mismatch:
            lines.append(f"    - {col}")

    lines.append("")
    return "\n".join(lines)
