<div align="center">

# 🔍 dataflow-agent

**AI-powered data pipeline debugger**

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Gemini](https://img.shields.io/badge/Gemini-2.5_Flash-4285F4?style=for-the-badge&logo=google&logoColor=white)](https://aistudio.google.com/)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2%2B-1C3C3C?style=for-the-badge&logo=langchain&logoColor=white)](https://github.com/langchain-ai/langgraph)
[![License](https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge)](LICENSE)

<br/>

An agentic CLI tool that uses **Google Gemini** and **LangGraph** to autonomously
diagnose, explain, and fix broken or slow data pipelines.

<br/>

[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Prefect](https://img.shields.io/badge/Prefect-024DFD?style=flat-square&logo=prefect&logoColor=white)](https://www.prefect.io/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)](https://www.snowflake.com/)

</div>

---

## What it does

`dataflow-agent` acts as an autonomous data engineering assistant. Point it at a broken pipeline — a dbt error log, a failing Airflow DAG, a Spark OOM crash — and it will:

1. **Read** the logs and project files using its tool suite
2. **Reason** over the evidence with Gemini running inside a LangGraph ReAct loop
3. **Explain** the root cause in plain English
4. **Fix** the broken code — showing a rich diff and asking for your confirmation before writing

---

## Features

| Capability | Details |
|---|---|
| 🔎 **Autonomous diagnosis** | Reads logs, traverses project files, calls tools in sequence until it has enough evidence |
| 🛠 **Interactive fixes** | Rich unified diff preview with `y/n` confirmation before any file is modified |
| 🗄 **Schema introspection** | Queries live Postgres or Snowflake to validate column and table references |
| 📐 **SQL analysis** | sqlglot-powered parsing, linting, and optimization hints (indexes, skew, wildcards, etc.) |
| 💬 **Chat mode** | Multi-turn session with the full pipeline context retained across turns |
| 🧩 **Framework-aware** | Dedicated parsers for dbt artifacts, Airflow DAGs, Prefect flows, and Spark driver logs |
| 📊 **dbt project profiler** | Scans all SQL models, scores complexity (CTEs, JOINs, anti-patterns), ranks by risk |
| 🧪 **dbt test generator** | Infers and writes a `schema.yml` with `not_null`, `unique`, and FK tests from model SQL |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        dataflow-agent                           │
│                                                                 │
│   CLI (Typer + Rich)                                            │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  LangGraph Agent Graph                  │   │
│  │                                                         │   │
│  │   START → context_loader → agent_node ⇄ tools_node     │   │
│  │                                 │            │          │   │
│  │                            Gemini LLM   Tool Executor   │   │
│  │                                 │            │          │   │
│  │                                 └────────────┘          │   │
│  │                                      │                  │   │
│  │                                     END                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│   Tools available to the agent:                                 │
│   read_log · read_file · list_files · extract_errors            │
│   analyze_sql · explain_query · inspect_schema                  │
│   parse_dbt_manifest · generate_dbt_tests · profile_dbt_project │
│   parse_airflow_dag · parse_prefect_flow · parse_spark_log      │
│   write_fix                                                     │
└─────────────────────────────────────────────────────────────────┘
```

The agent loops — calling tools, processing results, calling more tools — until Gemini decides it has enough information to produce a final diagnosis. No hardcoded logic, no fixed pipelines.

---

## Installation

**Requirements:** Python 3.11+, a [free Gemini API key](https://aistudio.google.com/app/apikey)

```bash
# 1. Clone
git clone https://github.com/yourname/dataflow-agent
cd dataflow-agent

# 2. Virtual environment
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

# 3. Install
pip install -e .

# 4. Configure
cp .env.example .env
#  → open .env and paste your GEMINI_API_KEY
```

---

## Configuration

```env
# .env

# Required
GEMINI_API_KEY=your_gemini_api_key_here

# Optional — defaults shown
GEMINI_MODEL=gemini-2.5-flash

# Optional — for schema introspection
POSTGRES_URL=postgresql://user:password@localhost:5432/mydb

SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=myuser
SNOWFLAKE_PASSWORD=mypassword
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

---

## Usage

### `diagnose` — find the root cause

```bash
# dbt run log
dataflow-agent diagnose --framework dbt --log ./logs/dbt.log

# dbt log + full project (agent can traverse models, macros, etc.)
dataflow-agent diagnose --framework dbt \
  --log ./logs/dbt.log \
  --project ./my_dbt_project

# Airflow — DAG file + task log
dataflow-agent diagnose --framework airflow \
  --dag ./dags/my_dag.py \
  --log ./logs/task.log

# Spark — OOM, executor loss, shuffle failures
dataflow-agent diagnose --framework spark \
  --log ./logs/spark_driver.log
```

### `diagnose --fix` — diagnose and repair

```bash
# Shows a rich diff, asks y/n before writing
dataflow-agent diagnose --framework dbt \
  --log ./logs/dbt.log \
  --project ./my_dbt_project \
  --fix
```

Example output:

```
╭─ Proposed change ──────────────────────────────────╮
│ --- a/models/marts/fct_orders.sql                  │
│ +++ b/models/marts/fct_orders.sql                  │
│ @@ -11,7 +11,7 @@                                  │
│ -    SUM(oi.discount_amount) AS total_discount,    │
│ +    SUM(oi.discount_pct)    AS total_discount,    │
╰────────────────────────────────────────────────────╯
Apply this fix? [y/N]:
```

### `optimize` — find performance issues

```bash
dataflow-agent optimize --framework spark \
  --log ./logs/spark.log \
  --db postgres
```

### `chat` — interactive session

```bash
# Full context retained across turns — ask follow-up questions
dataflow-agent chat --framework dbt --project ./my_dbt_project

dataflow-agent chat --framework spark --log ./logs/spark.log
```

```
╭─ Interactive Pipeline Chat ────────────────────────╮
│ Framework: dbt  |  Type exit to end the session.   │
╰────────────────────────────────────────────────────╯

You> Why did fct_orders fail?
Agent> The model failed because it references `discount_amount`, but
       the upstream `int_order_items` model exposes `discount_pct`...

You> What other models depend on fct_orders?
Agent> Based on the manifest, three models reference fct_orders: ...
```

### `profile` — rank dbt models by complexity

Scans every `.sql` file under `models/`, scores each by CTEs, JOINs, subqueries, and anti-patterns, and returns a ranked report with LLM-generated refactoring recommendations.

```bash
dataflow-agent profile ./my_dbt_project

# Show top 20 models instead of the default 10
dataflow-agent profile ./my_dbt_project --top 20
```

### `generate-tests` — generate a `schema.yml` from model SQL

Infers `not_null`, `unique`, and foreign-key tests from a model's SQL and writes a ready-to-use `schema.yml`.

```bash
dataflow-agent generate-tests --model ./models/marts/fct_orders.sql

# Write to a specific output path
dataflow-agent generate-tests \
  --model ./models/marts/fct_orders.sql \
  --output ./models/marts/schema.yml
```

### `--model` — override the LLM

```bash
dataflow-agent diagnose --framework dbt --log ./dbt.log --model gemini-1.5-pro
```

---

## Demo — try it now

The repo ships with realistic broken fixtures. No database needed.

```bash
# dbt: two failed models with column name mismatches
dataflow-agent diagnose --framework dbt \
  --log tests/fixtures/dbt_error.log

# Spark: executor OOM → stage failure → job abort
dataflow-agent diagnose --framework spark \
  --log tests/fixtures/spark_error.log

# Airflow: KeyError on a renamed column + catchup=True misconfiguration
dataflow-agent diagnose --framework airflow \
  --dag  tests/fixtures/broken_dag.py \
  --log  tests/fixtures/airflow_error.log

# Interactive chat about the broken Airflow DAG
dataflow-agent chat --framework airflow \
  --dag tests/fixtures/broken_dag.py
```

---

## Project Structure

```
dataflow-agent/
├── dataflow_agent/
│   ├── cli.py                   # Typer CLI: diagnose · optimize · chat · profile · generate-tests
│   ├── agent.py                 # LangGraph graph, state, nodes, chat loop
│   ├── config.py                # Pydantic settings loaded from .env
│   ├── tools/
│   │   ├── log_reader.py        # read_log
│   │   ├── file_reader.py       # read_file · list_files
│   │   ├── file_writer.py       # write_fix  (Rich diff + y/n prompt)
│   │   ├── schema_inspector.py  # inspect_schema  (Postgres + Snowflake)
│   │   ├── sql_analyzer.py      # analyze_sql · explain_query
│   │   ├── dbt_profiler.py      # profile_dbt_project  (complexity ranking)
│   │   └── framework/
│   │       ├── dbt.py           # parse_dbt_manifest · generate_dbt_tests
│   │       ├── airflow.py       # parse_airflow_dag
│   │       ├── prefect.py       # parse_prefect_flow
│   │       └── spark.py         # parse_spark_log
│   └── parsers/
│       └── error_extractor.py   # extract_errors  (framework-aware regex)
├── tests/
│   ├── fixtures/
│   │   ├── dbt_error.log        # dbt run with 2 failures + 5 skips
│   │   ├── airflow_error.log    # Airflow KeyError after 3 retry attempts
│   │   ├── spark_error.log      # Spark OOM → executor loss → job abort
│   │   ├── broken_dag.py        # Airflow DAG with column bug + catchup=True
│   │   ├── fct_orders.sql       # dbt model with wrong column reference
│   │   └── dbt_model.sql        # dbt model fixture for test generation
│   └── test_tools.py            # smoke tests (all passing)
├── .env.example
├── .gitignore
└── pyproject.toml
```

---

## Supported Frameworks

| Framework | Log parsing | File parsing | Schema validation | SQL analysis |
|---|---|---|---|---|
| **dbt** | ✅ Run logs | ✅ `manifest.json`, `run_results.json` | ✅ | ✅ |
| **Apache Airflow** | ✅ Task logs | ✅ DAG `.py` files | ✅ | ✅ |
| **Prefect** | ✅ Flow run logs | ✅ Flow `.py` files | ✅ | ✅ |
| **Apache Spark** | ✅ Java exceptions, OOM, shuffle | ✅ PySpark scripts | ✅ | ✅ |

---

## Tech Stack

| Layer | Library |
|---|---|
| LLM | `google-generativeai` / `langchain-google-genai` |
| Agent framework | `langgraph` |
| CLI | `typer` + `rich` |
| SQL parsing | `sqlglot` |
| DB connectors | `psycopg2-binary`, `snowflake-connector-python` |
| Config | `pydantic` + `python-dotenv` |

---

## License

MIT — see [LICENSE](LICENSE).
