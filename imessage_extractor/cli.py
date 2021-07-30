import logging
import click
import pydoni
from .src.run import run


@click.group()
def cli():
    """
    Command line interface for imessage-extractor.
    """
    pass


cli.add_command(run)


def main(args=None):
    cli()