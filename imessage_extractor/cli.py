import click
from imessage_extractor.src.go import go
from imessage_extractor.src.app.run_app import run_app


@click.group()
def cli():
    """
    Command line interface for imessage-extractor.
    """
    pass


cli.add_command(go)
cli.add_command(run_app)


def main(args=None):
    cli()