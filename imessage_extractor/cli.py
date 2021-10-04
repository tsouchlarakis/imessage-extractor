import click
from .src.go import go
from .src.refresh_contacts import refresh_contacts


@click.group()
def cli():
    """
    Command line interface for imessage-extractor.
    """
    pass


cli.add_command(go)
cli.add_command(refresh_contacts)


def main(args=None):
    cli()