"""Basic smoke tests for dataflow-agent tools."""
import pytest
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def test_read_log_missing_file():
    from dataflow_agent.tools.log_reader import read_log
    result = read_log.invoke({"path": "/nonexistent/path.log"})
    assert "ERROR" in result


def test_read_log_dbt_fixture():
    from dataflow_agent.tools.log_reader import read_log
    result = read_log.invoke({"path": str(FIXTURES / "dbt_error.log")})
    assert "discount_amount" in result
    assert "fct_orders" in result


def test_extract_errors_dbt():
    from dataflow_agent.parsers.error_extractor import extract_errors
    log_text = (FIXTURES / "dbt_error.log").read_text()
    result = extract_errors.invoke({"log_text": log_text, "framework": "dbt"})
    assert "discount_amount" in result
    assert "Snippet" in result


def test_extract_errors_airflow():
    from dataflow_agent.parsers.error_extractor import extract_errors
    log_text = (FIXTURES / "airflow_error.log").read_text()
    result = extract_errors.invoke({"log_text": log_text, "framework": "airflow"})
    assert "KeyError" in result or "Traceback" in result


def test_extract_errors_spark():
    from dataflow_agent.parsers.error_extractor import extract_errors
    log_text = (FIXTURES / "spark_error.log").read_text()
    result = extract_errors.invoke({"log_text": log_text, "framework": "spark"})
    assert "OutOfMemoryError" in result or "Snippet" in result


def test_parse_spark_log():
    from dataflow_agent.tools.framework.spark import parse_spark_log
    result = parse_spark_log.invoke({"path": str(FIXTURES / "spark_error.log")})
    assert "OutOfMemoryError" in result
    assert "CRITICAL" in result


def test_parse_airflow_dag():
    from dataflow_agent.tools.framework.airflow import parse_airflow_dag
    result = parse_airflow_dag.invoke({"path": str(FIXTURES / "broken_dag.py")})
    assert "daily_sales_pipeline" in result
    assert "catchup=True" in result


def test_list_files():
    from dataflow_agent.tools.file_reader import list_files
    result = list_files.invoke({"directory": str(FIXTURES), "pattern": "*.log"})
    assert "dbt_error.log" in result
    assert "spark_error.log" in result


def test_analyze_sql_select_star():
    from dataflow_agent.tools.sql_analyzer import analyze_sql
    result = analyze_sql.invoke({"query": "SELECT * FROM orders", "dialect": "postgres"})
    assert "SELECT *" in result or "Avoid SELECT" in result


def test_analyze_sql_no_where():
    from dataflow_agent.tools.sql_analyzer import analyze_sql
    result = analyze_sql.invoke({"query": "SELECT id, name FROM customers", "dialect": "postgres"})
    assert "WHERE" in result or "filter" in result.lower()


def test_explain_query_rejects_non_select():
    from dataflow_agent.tools.sql_analyzer import explain_query
    result = explain_query.invoke({
        "query": "DROP TABLE orders",
        "db_type": "postgres",
        "connection_string": "postgresql://user:pass@localhost/db",
    })
    assert "ERROR" in result
    assert "SELECT" in result


def test_explain_query_bad_db_type():
    from dataflow_agent.tools.sql_analyzer import explain_query
    result = explain_query.invoke({
        "query": "SELECT 1",
        "db_type": "mysql",
        "connection_string": "mysql://user:pass@localhost/db",
    })
    assert "ERROR" in result
    assert "postgres" in result.lower() or "snowflake" in result.lower()


def test_explain_query_postgres_connection_error():
    from dataflow_agent.tools.sql_analyzer import explain_query
    result = explain_query.invoke({
        "query": "SELECT id FROM orders WHERE status = 'active'",
        "db_type": "postgres",
        "connection_string": "postgresql://fake:fake@localhost:9999/fakedb",
    })
    assert "ERROR" in result


def test_slow_query_fixture_analyze():
    from dataflow_agent.tools.sql_analyzer import analyze_sql
    sql = (FIXTURES / "slow_query.sql").read_text()
    # Strip SQL comments before parsing
    import re
    sql_clean = re.sub(r"--[^\n]*", "", sql).strip()
    result = analyze_sql.invoke({"query": sql_clean, "dialect": "postgres"})
    assert "SELECT *" in result or "Avoid SELECT" in result
    assert "ORDER BY" in result or "LIMIT" in result or "sort" in result.lower()
