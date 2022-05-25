import click
import subprocess
from os import chdir
from os.path import dirname


@click.command()
def run_app():
    """
    Run Streamlit-based iMessage Extractor visual analytics app.
    """
    chdir(dirname(__file__))
    subprocess.call(['streamlit', 'run', 'app.py'])
