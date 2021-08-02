import sqlite3
import click
import time
import pydoni
import logging
from .verbosity import print_startup_message, logger_setup, path, bold
from os import makedirs
from os.path import expanduser, isfile, isdir
from .extract_sqlite import extract_sqlite


logger = logger_setup(name='imessage-extractor', level=logging.ERROR)


@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
@click.option('--chat-db-path', type=str, default=expanduser('~/Library/Messages/chat.db'),
              help='Path to working chat.db.')
@click.option('--save-csv', type=str, default=None, required=False,
              help='Path to folder to save chat.db tables to.')
@click.option('--save-pg-schema', type=str, default=None, required=False,
              help='Name of Postgres schema to save tables to.')
@click.option('--pg-credentials', type=str, default='~/.pgpass', required=False,
              help=pydoni.advanced_strip("""EITHER the path to a local Postgres credentials
              file 'i.e. ~/.pgpass', OR a string with the connection credentials. Must
              be in format 'hostname:port:db_name:user:pg_pass'."""))

@click.command()
def go(chat_db_path, verbose, save_csv, save_pg_schema, pg_credentials):
    """
    Run the imessage-extractor.
    """
    # Configure logger
    logging_level = logging.INFO if verbose else logging.ERROR
    logger = logger_setup(name='imessage-extractor', level=logging_level)

    # Log parameter values
    params = {k: v for k, v in locals().items() if k not in ['logger', 'logging_level']}
    for name, value in params.items():
        logger.debug(f'{name}: {value}')

    # Begin pipeline stopwatch
    start_ts = time.time()

    if verbose:
        print_startup_message(logger)

    logger.info(f"->->->->->->->->->->->->-> {click.style('Extract', bold=True)} <-<-<-<-<-<-<-<-<-<-<-<-<-")

    # Connect to the local chat.db
    if isfile(chat_db_path):
        try:
            sqlite_con = sqlite3.connect(chat_db_path)
            logger.info(f'Connected to chat.db {path(chat_db_path)}')
        except Exception as e:
            raise(Exception(pydoni.advanced_strip("""Unable to connect to SQLite! Could it be
            that the executing environment does not have proper permissions? Perhaps wrapping
            the command in an (Automator) application or script, and granting Full Disk
            Access to that application or script might be a potential option""")))
    else:
        raise FileNotFoundError(f'The chat.db file expected at {path(chat_db_path)} could not be found')

    # Returns a `ChatDbExtract` object containing a list of `ChatDbTable` objects
    chat_db_extract = extract_sqlite(logger=logger, sqlite_con=sqlite_con)

    logger.info(f"->->->->->->->->->->->->-> {click.style('Save', bold=True)} <-<-<-<-<-<-<-<-<-<-<-<-<-")

    # Save tables to local CSV folder
    if save_csv is not None:
        save_csv = expanduser(save_csv)
        if not isdir(save_csv):
            makedirs(save_csv)

        logger.info(f'Saving tables as CSV files at path {path(save_csv)}')
        chat_db_extract.save_to_csv(dir_name=save_csv, verbose=verbose, logger=logger)
    else:
        logger.info('User opted not to save tables as local CSV files')

    # Save tables to Postgres database
    if save_pg_schema is not None:
        if isfile(expanduser(pg_credentials)):
            with open(expanduser(pg_credentials), 'r') as f:
                pg_cred_str = f.read()
        else:
            pg_cred_str = pg_credentials

        assert len(pg_cred_str.split(':')) == 5, \
            pydoni.advanced_strip("""Invalid structure of supplied Postgres credentials.
            Must be either a path to a local Postgres credentials file 'i.e. ~/.pgpass', OR
            a string with the connection credentials. Must be in format
            'hostname:port:db_name:user_name:password'.""")

        hostname, port, db_name, user, pw = pg_cred_str.split(':')
        pg = pydoni.Postgres(hostname=hostname,
                             port=port,
                             db_name=db_name,
                             user=user,
                             pw=pw)

        logger.info(f'Connected to Postgres database {bold(db_name)} hosted on {bold(hostname + ":" + port)}')


    else:
        logger.info('User opted not to save tables to a Postgres database')

