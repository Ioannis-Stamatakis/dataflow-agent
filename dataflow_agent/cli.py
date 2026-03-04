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


if __name__ == "__main__":
    app()
