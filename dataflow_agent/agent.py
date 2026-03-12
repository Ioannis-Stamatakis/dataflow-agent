from __future__ import annotations

from typing import Annotated, TypedDict

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from dataflow_agent.config import config
from dataflow_agent.tools.file_reader import list_files, read_file
from dataflow_agent.tools.log_reader import read_log

console = Console()

# ---------------------------------------------------------------------------
# Lazy imports for optional heavy tools (avoids import errors when DB libs
# are not installed or credentials not set)
# ---------------------------------------------------------------------------

def _load_all_tools() -> list:
    tools = [read_log, read_file, list_files]

    try:
        from dataflow_agent.parsers.error_extractor import extract_errors
        tools.append(extract_errors)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.sql_analyzer import analyze_sql, explain_query
        tools.append(analyze_sql)
        tools.append(explain_query)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.schema_inspector import inspect_schema
        tools.append(inspect_schema)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.framework.dbt import parse_dbt_manifest, generate_dbt_tests, trace_dbt_lineage
        tools.append(parse_dbt_manifest)
        tools.append(generate_dbt_tests)
        tools.append(trace_dbt_lineage)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.framework.airflow import parse_airflow_dag
        tools.append(parse_airflow_dag)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.framework.prefect import parse_prefect_flow
        tools.append(parse_prefect_flow)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.framework.spark import parse_spark_log
        tools.append(parse_spark_log)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.file_writer import write_fix
        tools.append(write_fix)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.drift_detector import detect_schema_drift
        tools.append(detect_schema_drift)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.dbt_profiler import profile_dbt_project
        tools.append(profile_dbt_project)
    except ImportError:
        pass

    try:
        from dataflow_agent.tools.drift_detector import detect_schema_drift
        tools.append(detect_schema_drift)
    except ImportError:
        pass

    return tools


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    framework: str
    project_path: str | None
    fix_mode: bool
    diagnosis: str | None


# ---------------------------------------------------------------------------
# System prompt builder
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are dataflow-agent, an expert AI assistant for diagnosing, explaining, and fixing broken or slow data pipelines.

You support the following frameworks: dbt, Apache Airflow, Prefect, and Apache Spark.
You can introspect PostgreSQL and Snowflake schemas, parse SQL queries, and read log files.

## Your approach
1. Read any provided log files or project files first.
2. Extract and analyze errors using available tools.
3. Identify the root cause with a clear explanation.
4. If fix_mode is enabled, propose specific code changes using write_fix.
5. Always explain your reasoning in plain English that a data engineer would find actionable.

## Output format
- Lead with a short **Root Cause** summary.
- Follow with **Explanation** (what went wrong and why).
- Then **Recommended Fix** (concrete steps or code changes).
- If you cannot determine the cause, say so and list what additional information would help.

Current framework: {framework}
Fix mode: {fix_mode}
"""


def _build_system_prompt(framework: str, fix_mode: bool) -> SystemMessage:
    return SystemMessage(
        content=SYSTEM_PROMPT.format(framework=framework, fix_mode=fix_mode)
    )


# ---------------------------------------------------------------------------
# Graph nodes
# ---------------------------------------------------------------------------

def context_loader(state: AgentState) -> AgentState:
    """Pre-process: no-op pass-through (context is injected via initial messages)."""
    return state


def make_agent_node(llm_with_tools):
    def agent_node(state: AgentState) -> AgentState:
        response = llm_with_tools.invoke(state["messages"])
        return {"messages": [response]}
    return agent_node


def should_continue(state: AgentState) -> str:
    last = state["messages"][-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"
    return END


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def _build_graph(tools: list):
    llm = ChatGoogleGenerativeAI(
        model=config.gemini_model,
        google_api_key=config.gemini_api_key,
        temperature=0,
    )
    llm_with_tools = llm.bind_tools(tools)

    tool_node = ToolNode(tools)

    graph = StateGraph(AgentState)
    graph.add_node("context_loader", context_loader)
    graph.add_node("agent_node", make_agent_node(llm_with_tools))
    graph.add_node("tools_node", tool_node)

    graph.add_edge(START, "context_loader")
    graph.add_edge("context_loader", "agent_node")
    graph.add_conditional_edges("agent_node", should_continue, {"tools": "tools_node", END: END})
    graph.add_edge("tools_node", "agent_node")

    return graph.compile()


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def _build_initial_messages(
    task: str,
    framework: str,
    log_path: str | None,
    project_path: str | None,
    dag_path: str | None,
    db_type: str | None,
    fix_mode: bool,
    extra_message: str | None = None,
) -> list[BaseMessage]:
    system = _build_system_prompt(framework, fix_mode)

    parts = [f"Task: **{task}** for a **{framework}** pipeline."]
    if log_path:
        parts.append(f"Log file to analyze: `{log_path}`")
    if project_path:
        parts.append(f"Project directory: `{project_path}`")
    if dag_path:
        parts.append(f"DAG/flow file: `{dag_path}`")
    if db_type:
        parts.append(f"Database type: `{db_type}`")
    if fix_mode:
        parts.append("Fix mode is **enabled** — propose and apply fixes.")
    if extra_message:
        parts.append(extra_message)

    parts.append(
        "\nPlease begin your analysis. Use your tools to read any relevant files, "
        "then provide your diagnosis."
    )

    human = HumanMessage(content="\n".join(parts))
    return [system, human]


def run_agent(
    task: str,
    framework: str,
    log_path: str | None = None,
    project_path: str | None = None,
    dag_path: str | None = None,
    db_type: str | None = None,
    fix_mode: bool = False,
) -> None:
    tools = _load_all_tools()
    graph = _build_graph(tools)

    initial_messages = _build_initial_messages(
        task=task,
        framework=framework,
        log_path=log_path,
        project_path=project_path,
        dag_path=dag_path,
        db_type=db_type,
        fix_mode=fix_mode,
    )

    initial_state: AgentState = {
        "messages": initial_messages,
        "framework": framework,
        "project_path": project_path,
        "fix_mode": fix_mode,
        "diagnosis": None,
    }

    console.print("[dim]Running agent...[/dim]")

    final_state = graph.invoke(initial_state)
    last_message = final_state["messages"][-1]

    console.print()
    console.print(
        Panel(
            Markdown(last_message.content),
            title=f"[bold green]Diagnosis — {framework}[/bold green]",
            border_style="green",
        )
    )


def run_chat(
    framework: str,
    log_path: str | None = None,
    project_path: str | None = None,
    dag_path: str | None = None,
    db_type: str | None = None,
) -> None:
    tools = _load_all_tools()
    graph = _build_graph(tools)

    system = _build_system_prompt(framework, fix_mode=False)
    context_parts = [f"Starting interactive session for a **{framework}** pipeline."]
    if log_path:
        context_parts.append(f"Log file available: `{log_path}`")
    if project_path:
        context_parts.append(f"Project directory: `{project_path}`")
    if dag_path:
        context_parts.append(f"DAG/flow file: `{dag_path}`")
    if db_type:
        context_parts.append(f"Database: `{db_type}`")
    context_parts.append(
        "\nI'm ready to help. What would you like to know about this pipeline?"
    )

    messages: list[BaseMessage] = [
        system,
        HumanMessage(content="\n".join(context_parts)),
    ]

    state: AgentState = {
        "messages": messages,
        "framework": framework,
        "project_path": project_path,
        "fix_mode": False,
        "diagnosis": None,
    }

    # Get initial greeting
    result = graph.invoke(state)
    last = result["messages"][-1]
    console.print(Panel(Markdown(last.content), title="[bold magenta]Agent[/bold magenta]", border_style="magenta"))

    # Update state with full message history
    state = result

    while True:
        try:
            user_input = console.input("\n[bold cyan]You>[/bold cyan] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Session ended.[/dim]")
            break

        if user_input.lower() in ("exit", "quit", "q"):
            console.print("[dim]Goodbye![/dim]")
            break

        if not user_input:
            continue

        state["messages"] = list(state["messages"]) + [HumanMessage(content=user_input)]
        result = graph.invoke(state)
        last = result["messages"][-1]
        console.print(
            Panel(Markdown(last.content), title="[bold magenta]Agent[/bold magenta]", border_style="magenta")
        )
        state = result


def run_profile(project_path: str, top_n: int = 10) -> None:
    tools = _load_all_tools()
    graph = _build_graph(tools)

    system = SystemMessage(content=(
        "You are dataflow-agent, an expert dbt code reviewer and data engineer. "
        "You have access to the profile_dbt_project tool. "
        "When given a dbt project path:\n"
        "1. Call profile_dbt_project to get the complexity report.\n"
        "2. Synthesize the results into a clear, actionable summary with:\n"
        "   - **Project Health Overview** (one-paragraph verdict)\n"
        "   - **Top Risks** (the most complex/problematic models and why)\n"
        "   - **Quick Wins** (easy fixes that would reduce complexity immediately)\n"
        "   - **Refactoring Recommendations** (longer-term structural improvements)\n"
        "Be specific — name the actual model files and explain what makes each one risky."
    ))

    parts = [
        f"Please profile this dbt project: `{project_path}`",
        f"Show the top {top_n} most complex models.",
        "\nCall profile_dbt_project first, then give your full analysis.",
    ]

    initial_state: AgentState = {
        "messages": [system, HumanMessage(content="\n".join(parts))],
        "framework": "dbt",
        "project_path": project_path,
        "fix_mode": False,
        "diagnosis": None,
    }

    console.print("[dim]Profiling dbt project...[/dim]")
    final_state = graph.invoke(initial_state)
    last = final_state["messages"][-1]

    console.print()
    console.print(
        Panel(
            Markdown(last.content),
            title="[bold cyan]dbt Project Profile[/bold cyan]",
            border_style="cyan",
        )
    )


def run_explain(
    sql: str,
    db_type: str,
    connection_string: str,
    dialect: str = "",
) -> None:
    tools = _load_all_tools()
    graph = _build_graph(tools)

    system = SystemMessage(content=(
        "You are dataflow-agent, an expert SQL performance analyst. "
        "You have access to explain_query and analyze_sql tools. "
        "When given a SQL query and database credentials:\n"
        "1. Call explain_query to get the live query plan from the database.\n"
        "2. Call analyze_sql to get static linting and optimization hints.\n"
        "3. Synthesize both into a clear, actionable report with:\n"
        "   - **Performance Summary** (one sentence verdict)\n"
        "   - **Query Plan Findings** (what the planner is doing and why it's slow)\n"
        "   - **Optimization Recommendations** (specific, prioritized action items)\n"
        "   - **Rewritten Query** (if meaningful improvements are possible)\n"
        "Be specific — mention table names, index names, and execution times from the plan."
    ))

    parts = [
        f"Please explain and optimize this SQL query against a **{db_type}** database.\n",
        f"**Connection string:** `{connection_string}`",
        f"**Dialect:** `{dialect or db_type}`\n",
        "**Query:**",
        f"```sql\n{sql}\n```",
        "\nStart by calling explain_query, then analyze_sql, then give your full report.",
    ]

    initial_state: AgentState = {
        "messages": [system, HumanMessage(content="\n".join(parts))],
        "framework": "sql",
        "project_path": None,
        "fix_mode": False,
        "diagnosis": None,
    }

    console.print("[dim]Running EXPLAIN ANALYZE...[/dim]")
    final_state = graph.invoke(initial_state)
    last = final_state["messages"][-1]

    console.print()
    console.print(
        Panel(
            Markdown(last.content),
            title="[bold yellow]Query Explanation[/bold yellow]",
            border_style="yellow",
        )
    )


def run_drift(
    schema_yml_path: str,
    db_type: str,
    connection_string: str,
    model_name: str = "",
) -> None:
    from dataflow_agent.tools.drift_detector import detect_schema_drift

    result = detect_schema_drift.invoke({
        "schema_yml_path": schema_yml_path,
        "db_type": db_type,
        "connection_string": connection_string,
        "model_name": model_name,
    })

    console.print()
    console.print(
        Panel(
            result,
            title="[bold magenta]Schema Drift Report[/bold magenta]",
            border_style="magenta",
        )
    )


def run_lineage(
    model_name: str,
    project_path: str = "",
    manifest_path: str = "",
    direction: str = "both",
    depth: int = -1,
    analyze: bool = False,
    llm_model: str | None = None,
) -> None:
    from dataflow_agent.tools.framework.dbt import trace_dbt_lineage

    if llm_model:
        config.gemini_model = llm_model

    if not analyze:
        result = trace_dbt_lineage.invoke({
            "model_name": model_name,
            "project_path": project_path,
            "manifest_path": manifest_path,
            "direction": direction,
            "depth": depth,
        })
        console.print()
        console.print(
            Panel(
                result,
                title=f"[bold blue]dbt Lineage — {model_name}[/bold blue]",
                border_style="blue",
            )
        )
        return

    # AI-powered analysis
    tools = _load_all_tools()
    graph = _build_graph(tools)

    system = SystemMessage(content=(
        "You are dataflow-agent, an expert dbt data engineer and lineage analyst. "
        "You have access to the trace_dbt_lineage tool. "
        "When asked to analyze a model's lineage:\n"
        "1. Call trace_dbt_lineage to retrieve the full dependency graph.\n"
        "2. Synthesize the results into a clear impact analysis with:\n"
        "   - **Upstream Risk** (which sources/models this depends on and where failures could cascade from)\n"
        "   - **Downstream Impact** (what breaks if this model changes or fails)\n"
        "   - **Testing Gaps** (layers of the graph with insufficient test coverage)\n"
        "   - **Recommendations** (concrete steps to improve reliability and observability)\n"
        "Be specific — name the actual models and explain their roles."
    ))

    parts = [
        f"Please analyze the lineage for dbt model: `{model_name}`",
    ]
    if manifest_path:
        parts.append(f"Manifest path: `{manifest_path}`")
    if project_path:
        parts.append(f"Project path: `{project_path}`")
    parts.append(f"Direction: `{direction}`  |  Depth: `{depth}`")
    parts.append("\nCall trace_dbt_lineage first, then give your full impact analysis.")

    initial_state: AgentState = {
        "messages": [system, HumanMessage(content="\n".join(parts))],
        "framework": "dbt",
        "project_path": project_path or None,
        "fix_mode": False,
        "diagnosis": None,
    }

    console.print("[dim]Analyzing lineage...[/dim]")
    final_state = graph.invoke(initial_state)
    last = final_state["messages"][-1]

    console.print()
    console.print(
        Panel(
            Markdown(last.content),
            title=f"[bold blue]dbt Lineage Analysis — {model_name}[/bold blue]",
            border_style="blue",
        )
    )


def run_drift(
    schema_yml_path: str,
    db_type: str,
    connection_string: str,
    model_name: str = "",
    analyze: bool = False,
) -> None:
    from dataflow_agent.tools.drift_detector import detect_schema_drift

    if not analyze:
        result = detect_schema_drift.invoke({
            "schema_yml_path": schema_yml_path,
            "db_type": db_type,
            "connection_string": connection_string,
            "model_name": model_name,
        })
        console.print()
        console.print(
            Panel(
                result,
                title="[bold red]Schema Drift Report[/bold red]",
                border_style="red",
            )
        )
        return

    # AI-powered remediation analysis
    tools = _load_all_tools()
    graph = _build_graph(tools)

    system = SystemMessage(content=(
        "You are dataflow-agent, an expert dbt data engineer specializing in schema management. "
        "You have access to the detect_schema_drift tool. "
        "When asked to analyze schema drift:\n"
        "1. Call detect_schema_drift to retrieve the full drift report.\n"
        "2. Synthesize the findings into an actionable remediation plan with:\n"
        "   - **Drift Summary** (one-paragraph overview of what drifted and likely causes)\n"
        "   - **Missing Columns** (columns in schema.yml not in DB — were they dropped? Should they be removed from schema.yml?)\n"
        "   - **Undocumented Columns** (columns in DB not in schema.yml — should they be added with tests?)\n"
        "   - **Type Mismatches** (explain the risk and suggest ALTER TABLE or schema.yml corrections)\n"
        "   - **Recommended Actions** (prioritized, concrete steps with code snippets where helpful)\n"
        "Be specific — name the actual columns and suggest exact schema.yml patches or SQL statements."
    ))

    parts = [
        f"Please analyze schema drift for: `{schema_yml_path}`",
        f"Database type: `{db_type}`",
        f"Connection: `{connection_string}`",
    ]
    if model_name:
        parts.append(f"Model: `{model_name}`")
    parts.append("\nCall detect_schema_drift first, then give your full remediation plan.")

    initial_state: AgentState = {
        "messages": [system, HumanMessage(content="\n".join(parts))],
        "framework": "dbt",
        "project_path": None,
        "fix_mode": False,
        "diagnosis": None,
    }

    console.print("[dim]Detecting schema drift...[/dim]")
    final_state = graph.invoke(initial_state)
    last = final_state["messages"][-1]

    console.print()
    console.print(
        Panel(
            Markdown(last.content),
            title="[bold red]Schema Drift Analysis[/bold red]",
            border_style="red",
        )
    )
