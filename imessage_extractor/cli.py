import logging
import click
from .src.go import go


@click.group()
def cli():
    """
    Command line interface for imessage-extractor.
    """
    pass


cli.add_command(go)


def main(args=None):
    cli()