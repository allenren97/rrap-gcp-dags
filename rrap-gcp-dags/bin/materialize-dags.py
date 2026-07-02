#!/usr/bin/env python3
"""Generate fully-expanded DAG files for Airflow Code tab visibility.

This tool expands `deduped_imports(...)` and `import_contents(...)` markers from
template DAG files into plain Python DAG files written to the output DAG folder.
"""

from __future__ import annotations

from pathlib import Path
import click

from generator.generate_dags import generate_dags


@click.command()
@click.option(
    "--source-dir",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=Path("dag_templates"),
    show_default=True,
    help="Directory containing DAG template files.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=Path("dags"),
    show_default=True,
    help="Directory where expanded DAG files are written.",
)
@click.option(
    "--entry-glob",
    default="*.py",
    show_default=True,
    help="Glob for DAG entry templates inside source-dir.",
)
@click.option("--strict/--no-strict", default=False, show_default=True)
@click.option("--clean/--no-clean", default=True, show_default=True)
def cli(
    source_dir: Path,
    output_dir: Path,
    entry_glob: str,
    strict: bool,
    clean: bool,
) -> None:
    """Generate fully-expanded DAG files from templates."""
    written = generate_dags(
        source_dir=source_dir,
        output_dir=output_dir,
        entry_glob=entry_glob,
        strict=strict,
        clean=clean,
    )
    click.echo(f"Generated {len(written)} DAG file(s):")
    for path in written:
        click.echo(f"- {path}")


if __name__ == "__main__":
    cli()

