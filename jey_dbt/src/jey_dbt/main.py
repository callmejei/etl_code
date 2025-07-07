import typer
from jey_dbt.cli import dbt_cli

cli=typer.Typer()
cli.add_typer(dbt_cli.cli, name="dbt")

@cli.command()
def hello(name: str):
    """hello command for jey dbt cli"""
    print(f"Hello, {name}")