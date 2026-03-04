"""
Broken Airflow DAG — fixture for testing dataflow-agent.

Issues present:
1. References 'discount_rate' column instead of 'discount_pct'
2. catchup=True can trigger unwanted backfills
3. No retries configured on the failing task
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # BUG: no retries
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="daily_sales_pipeline",
    default_args=default_args,
    description="Daily sales data pipeline",
    schedule_interval="0 9 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,  # BUG: will trigger many backfill runs
    tags=["sales", "daily"],
)


def extract_orders(**context):
    """Extract orders from source PostgreSQL database."""
    conn = psycopg2.connect("postgresql://etl_user:secret@prod-db:5432/sales")
    df = pd.read_sql(
        "SELECT order_id, customer_id, quantity, unit_price, discount_pct, order_date, status "
        "FROM orders WHERE order_date = %(exec_date)s",
        conn,
        params={"exec_date": context["ds"]},
    )
    conn.close()
    df.to_parquet(f"/tmp/orders_{context['ds']}.parquet", index=False)
    return len(df)


def transform_orders(**context):
    """Transform orders: compute revenue."""
    df = pd.read_parquet(f"/tmp/orders_{context['ds']}.parquet")

    # BUG: column is 'discount_pct' not 'discount_rate'
    df['revenue'] = df['quantity'] * df['unit_price'] * (1 - df['discount_rate'])
    df['revenue_category'] = pd.cut(
        df['revenue'],
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['small', 'medium', 'large', 'enterprise']
    )

    df.to_parquet(f"/tmp/orders_transformed_{context['ds']}.parquet", index=False)
    return len(df)


def load_orders(**context):
    """Load transformed orders to data warehouse."""
    df = pd.read_parquet(f"/tmp/orders_transformed_{context['ds']}.parquet")
    conn = psycopg2.connect("postgresql://dw_user:secret@dw-db:5432/warehouse")
    df.to_sql("fct_orders", conn, schema="analytics", if_exists="append", index=False)
    conn.close()


extract_task = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_orders",
    python_callable=transform_orders,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_orders",
    python_callable=load_orders,
    dag=dag,
)

extract_task >> transform_task >> load_task
