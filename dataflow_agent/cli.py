from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel

from dataflow_agent.config import config

app = typer.Typer(
    name="dataflow-agent",
    help="AI-powered data pipeline debugger using Gemini and LangGraph.",
    add_completion=False,
)
console = Console()

FRAMEWORK_CHOICES = ["dbt", "airflow", "prefect", "spark"]


def _validate_framework(framework: str) -> str:
    if framework not in FRAMEWORK_CHOICES:
        console.print(
            f"[red]Invalid framework '{framework}'. "
            f"Choose from: {', '.join(FRAMEWORK_CHOICES)}[/red]"
        )
        raise typer.Exit(1)
    return framework


@app.command()
def diagnose(
    framework: str = typer.Option(..., help="Pipeline framework: dbt|airflow|prefect|spark"),
    log: Optional[Path] = typer.Option(None, help="Path to log file"),
    project: Optional[Path] = typer.Option(None, help="Path to project directory"),
    dag: Optional[Path] = typer.Option(None, help="Path to DAG or flow file"),
    db: Optional[str] = typer.Option(None, help="Database type: postgres|snowflake"),
    fix: bool = typer.Option(False, "--fix", help="Propose and apply fixes interactively"),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Diagnose failures in a data pipeline."""
    _validate_framework(framework)
    config.require_gemini_key()

    if model:
        config.gemini_model = model

    if not log and not project and not dag:
        console.print("[red]Provide at least one of --log, --project, or --dag.[/red]")
        raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent diagnose[/bold cyan]\n"
            f"Framework: [yellow]{framework}[/yellow]  |  Fix mode: [yellow]{fix}[/yellow]",
            title="Diagnosing Pipeline",
            border_style="blue",
        )
    )

    from dataflow_agent.agent import run_agent

    run_agent(
        task="diagnose",
        framework=framework,
        log_path=str(log) if log else None,
        project_path=str(project) if project else None,
        dag_path=str(dag) if dag else None,
        db_type=db,
        fix_mode=fix,
    )


@app.command()
def optimize(
    framework: str = typer.Option(..., help="Pipeline framework: dbt|airflow|prefect|spark"),
    log: Optional[Path] = typer.Option(None, help="Path to log file"),
    project: Optional[Path] = typer.Option(None, help="Path to project directory"),
    db: Optional[str] = typer.Option(None, help="Database type: postgres|snowflake"),
    fix: bool = typer.Option(False, "--fix", help="Propose and apply optimizations interactively"),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Optimize a slow data pipeline or query."""
    _validate_framework(framework)
    config.require_gemini_key()

    if model:
        config.gemini_model = model

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent optimize[/bold cyan]\n"
            f"Framework: [yellow]{framework}[/yellow]",
            title="Optimizing Pipeline",
            border_style="green",
        )
    )

    from dataflow_agent.agent import run_agent

    run_agent(
        task="optimize",
        framework=framework,
        log_path=str(log) if log else None,
        project_path=str(project) if project else None,
        db_type=db,
        fix_mode=fix,
    )


@app.command()
def chat(
    framework: str = typer.Option(..., help="Pipeline framework: dbt|airflow|prefect|spark"),
    log: Optional[Path] = typer.Option(None, help="Path to log file"),
    project: Optional[Path] = typer.Option(None, help="Path to project directory"),
    dag: Optional[Path] = typer.Option(None, help="Path to DAG or flow file"),
    db: Optional[str] = typer.Option(None, help="Database type: postgres|snowflake"),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Start an interactive chat session about a data pipeline."""
    _validate_framework(framework)
    config.require_gemini_key()

    if model:
        config.gemini_model = model

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent chat[/bold cyan]\n"
            f"Framework: [yellow]{framework}[/yellow]\n"
            "Type [bold]exit[/bold] or [bold]quit[/bold] to end the session.",
            title="Interactive Pipeline Chat",
            border_style="magenta",
        )
    )

    from dataflow_agent.agent import run_chat

    run_chat(
        framework=framework,
        log_path=str(log) if log else None,
        project_path=str(project) if project else None,
        dag_path=str(dag) if dag else None,
        db_type=db,
    )


@app.command(name="generate-tests")
def generate_tests(
    model: Path = typer.Option(..., "--model", "-m", help="Path to the dbt model .sql file"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output path for schema.yml (default: same dir as model)"),
    manifest: Optional[Path] = typer.Option(None, "--manifest", help="Optional path to manifest.json for FK cross-referencing"),
    model_override: Optional[str] = typer.Option(None, "--model-override", help="LLM model override"),
) -> None:
    """Generate a schema.yml with dbt tests inferred from a model's SQL."""
    if not model.exists():
        console.print(f"[red]Model file not found: {model}[/red]")
        raise typer.Exit(1)

    if model_override:
        config.gemini_model = model_override

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent generate-tests[/bold cyan]\n"
            f"Model: [yellow]{model}[/yellow]",
            title="Generating dbt Tests",
            border_style="cyan",
        )
    )

    from dataflow_agent.tools.framework.dbt import generate_dbt_tests

    result = generate_dbt_tests.invoke({
        "model_sql_path": str(model),
        "output_path": str(output) if output else "",
        "manifest_path": str(manifest) if manifest else "",
    })

    console.print(f"\n[green]{result.splitlines()[0]}[/green]")


@app.command()
def explain(
    query: Optional[str] = typer.Option(None, "--query", "-q", help="SQL query string to explain"),
    file: Optional[Path] = typer.Option(None, "--file", "-f", help="Path to a .sql file to explain"),
    db: str = typer.Option(..., help="Database type: postgres|snowflake"),
    connection: str = typer.Option(..., "--connection", "-c", help="Connection string (DSN for Postgres or account/user/pass/db/schema/wh for Snowflake)"),
    dialect: str = typer.Option("", help="SQL dialect hint for formatting: postgres, snowflake, spark, etc."),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Run EXPLAIN ANALYZE on a SQL query and get a plain-English interpretation."""
    config.require_gemini_key()

    if model:
        config.gemini_model = model

    if not query and not file:
        console.print("[red]Provide either --query or --file.[/red]")
        raise typer.Exit(1)

    if file:
        p = Path(file)
        if not p.exists():
            console.print(f"[red]File not found: {file}[/red]")
            raise typer.Exit(1)
        sql = p.read_text(encoding="utf-8").strip()
    else:
        sql = query.strip()

    if db.lower() not in ("postgres", "snowflake"):
        console.print("[red]--db must be 'postgres' or 'snowflake'.[/red]")
        raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent explain[/bold cyan]\n"
            f"Database: [yellow]{db}[/yellow]  |  Dialect: [yellow]{dialect or 'auto'}[/yellow]",
            title="Explaining Query",
            border_style="yellow",
        )
    )

    from dataflow_agent.agent import run_explain

    run_explain(sql=sql, db_type=db, connection_string=connection, dialect=dialect)


@app.command()
def profile(
    project: Path = typer.Argument(..., help="Path to the dbt project root"),
    top: int = typer.Option(10, "--top", "-n", help="Number of top complex models to show"),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Profile all SQL models in a dbt project and rank them by complexity."""
    config.require_gemini_key()

    if model:
        config.gemini_model = model

    if not project.exists():
        console.print(f"[red]Path not found: {project}[/red]")
        raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent profile[/bold cyan]\n"
            f"Project: [yellow]{project}[/yellow]  |  Top N: [yellow]{top}[/yellow]",
            title="Profiling dbt Project",
            border_style="cyan",
        )
    )

    from dataflow_agent.agent import run_profile

    run_profile(project_path=str(project), top_n=top)


@app.command()
def lineage(
    model_name: str = typer.Argument(..., help="dbt model name (without .sql)"),
    project: Optional[Path] = typer.Option(None, "-p", "--project", help="Path to dbt project root"),
    manifest: Optional[Path] = typer.Option(None, "-m", "--manifest", help="Path to dbt manifest.json"),
    direction: str = typer.Option("both", "-d", "--direction", help="upstream | downstream | both"),
    depth: int = typer.Option(-1, "--depth", help="Max traversal depth (-1 = unlimited)"),
    analyze: bool = typer.Option(False, "-a", "--analyze", help="AI-powered impact analysis (requires GEMINI_API_KEY)"),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Trace upstream/downstream lineage for a dbt model."""
    if not project and not manifest:
        console.print("[red]Provide at least one of --project or --manifest.[/red]")
        raise typer.Exit(1)

    if manifest and not manifest.exists():
        console.print(f"[red]Manifest not found: {manifest}[/red]")
        raise typer.Exit(1)

    if project and not project.exists():
        console.print(f"[red]Project path not found: {project}[/red]")
        raise typer.Exit(1)

    if direction not in ("upstream", "downstream", "both"):
        console.print("[red]--direction must be 'upstream', 'downstream', or 'both'.[/red]")
        raise typer.Exit(1)

    if analyze:
        config.require_gemini_key()

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent lineage[/bold cyan]\n"
            f"Model: [yellow]{model_name}[/yellow]  |  Direction: [yellow]{direction}[/yellow]  |  Depth: [yellow]{depth}[/yellow]",
            title="dbt Lineage",
            border_style="blue",
        )
    )

    from dataflow_agent.agent import run_lineage

    run_lineage(
        model_name=model_name,
        project_path=str(project) if project else "",
        manifest_path=str(manifest) if manifest else "",
        direction=direction,
        depth=depth,
        analyze=analyze,
        llm_model=model,
    )


@app.command()
def drift(
    schema: Path = typer.Option(..., "--schema", "-s", help="Path to schema.yml"),
    db: str = typer.Option(..., help="Database type: postgres|snowflake"),
    connection: str = typer.Option(..., "--connection", "-c", help="Connection string (DSN for Postgres or account/user/pass/db/schema/wh for Snowflake)"),
    model_name: Optional[str] = typer.Option(None, "--model", "-m", help="Specific model name to check (default: all models in file)"),
    analyze: bool = typer.Option(False, "-a", "--analyze", help="AI-powered remediation advice (requires GEMINI_API_KEY)"),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Detect schema drift between a dbt schema.yml and the live database."""
    if not schema.exists():
        console.print(f"[red]schema.yml not found: {schema}[/red]")
        raise typer.Exit(1)

    if db.lower() not in ("postgres", "snowflake"):
        console.print("[red]--db must be 'postgres' or 'snowflake'.[/red]")
        raise typer.Exit(1)

    if analyze:
        config.require_gemini_key()

    if model:
        config.gemini_model = model

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent drift[/bold cyan]\n"
            f"Schema: [yellow]{schema}[/yellow]  |  DB: [yellow]{db}[/yellow]"
            + (f"  |  Model: [yellow]{model_name}[/yellow]" if model_name else ""),
            title="Schema Drift Detection",
            border_style="red",
        )
    )

    from dataflow_agent.agent import run_drift

    run_drift(
        schema_yml_path=str(schema),
        db_type=db,
        connection_string=connection,
        model_name=model_name or "",
        analyze=analyze,
    )


@app.command()
def validate(
    project: Path = typer.Argument(..., help="Path to the dbt project root"),
    schema: Optional[Path] = typer.Option(None, "--schema", "-s", help="Explicit schema.yml path (default: auto-discover)"),
    model_name: Optional[str] = typer.Option(None, "--model", "-m", help="Specific model name to check"),
    suggest: bool = typer.Option(False, "--suggest", help="Use LLM to suggest missing tests (requires GEMINI_API_KEY)"),
    model: Optional[str] = typer.Option(None, help="LLM model override"),
) -> None:
    """Check dbt schema.yml test coverage across all models. Works offline."""
    if not project.exists():
        console.print(f"[red]Project path not found: {project}[/red]")
        raise typer.Exit(1)

    if schema and not schema.exists():
        console.print(f"[red]Schema file not found: {schema}[/red]")
        raise typer.Exit(1)

    if suggest:
        config.require_gemini_key()

    if model:
        config.gemini_model = model

    console.print(
        Panel(
            f"[bold cyan]dataflow-agent validate[/bold cyan]\n"
            f"Project: [yellow]{project}[/yellow]"
            + (f"  |  Model: [yellow]{model_name}[/yellow]" if model_name else ""),
            title="dbt Test Coverage Validation",
            border_style="green",
        )
    )

    from dataflow_agent.agent import run_validate

    run_validate(
        project_path=str(project),
        schema_path=str(schema) if schema else "",
        model_name=model_name or "",
        suggest=suggest,
    )


if __name__ == "__main__":
    app()
