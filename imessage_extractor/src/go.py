import json
import sqlite3
import click
import shutil
import time
import typing
import pathlib
import pydoni
import logging
from .verbosity import print_startup_message, logger_setup, path, bold
from os import makedirs, listdir
from os.path import expanduser, isfile, isdir, splitext, abspath, dirname, join
from .objects import ChatDbExtract, View


vw_dpath = abspath(join(dirname(__file__), 'views'))
logger = logger_setup(name='imessage-extractor', level=logging.ERROR)


def validate_parameters(params: dict) -> None:
    """
    Carry out assertions on parameters.
    """
    if params['save_csv'] is None:
        if params['save_pg_schema'] is None:
            raise ValueError('Must specify either --save-csv or --save-pg-schema')

    if (params['save_pg_schema'] is not None and params['pg_credentials'] is None) \
        or (params['save_pg_schema'] is None and params['pg_credentials'] is not None):
            raise ValueError('Must specify both --pg-credentials and --save-pg-schema if one is specified')

    # Log parameter values
    params = {k: v for k, v in locals().items() if k not in ['logger', 'logging_level']}
    for name, value in params.items():
        logger.debug(f'{name}: {value}')


def connect_to_chat_db(chat_db_path: typing.Union[str, pathlib.Path]) -> sqlite3.Connection:
    """
    Connect to local chat.db.
    """
    if isfile(chat_db_path):
        try:
            sqlite_con = sqlite3.connect(chat_db_path)
            logger.info(f'Connected to chat.db {path(chat_db_path)}')
            return sqlite_con
        except Exception as e:
            raise(Exception(pydoni.advanced_strip("""Unable to connect to SQLite! Could it be
            that the executing environment does not have proper permissions? Perhaps wrapping
            the command in an (Automator) application or script, and granting Full Disk
            Access to that application or script might be a potential option""")))
    else:
        raise FileNotFoundError(f'The chat.db file expected at {path(chat_db_path)} could not be found')


def parse_pg_credentials(pg_credentials: typing.Union[str, pathlib.Path]) -> tuple:
    """
    Parse Postgres connection credentials from either a string or a file.
    """
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

    hostname, port, db_name, pg_user, pw = pg_cred_str.split(':')
    return hostname, port, db_name, pg_user, pw


def list_view_names(vw_dpath: typing.Union[str, pathlib.Path]) -> list:
    """
    Find views in folder.
    """
    return [f.replace('.sql', '') for f in listdir(vw_dpath) if splitext(f)[1] == '.sql']


@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
@click.option('--chat-db-path', type=str, default=expanduser('~/Library/Messages/chat.db'),
              help='Path to working chat.db.')
@click.option('--save-csv', type=str, default=None, required=False,
              help='Path to folder to save chat.db tables to.')
@click.option('--save-pg-schema', type=str, default=None, required=False,
              help='Name of Postgres schema to save tables to.')
@click.option('--pg-credentials', type=str, default=expanduser('~/.pgpass'), required=False,
              help=pydoni.advanced_strip("""EITHER the path to a local Postgres credentials
              file 'i.e. ~/.pgpass', OR a string with the connection credentials. Must
              be in format 'hostname:port:db_name:user:pg_pass'."""))

@click.command()
def go(chat_db_path,
       verbose,
       save_csv,
       save_pg_schema,
       pg_credentials):
    """
    Run the imessage-extractor.
    """
    params = locals()

    # Configure logger
    logging_level = logging.INFO if verbose else logging.ERROR
    logger = logger_setup(name='imessage-extractor', level=logging_level)

    validate_parameters(params)

    # Begin pipeline stopwatch
    start_ts = time.time()

    if verbose:
        print_startup_message(logger)

    # Connect to the local chat.db
    sqlite_con = connect_to_chat_db(chat_db_path)

    # Create a temporary folder to save the extracted data to if the user opted not
    # to save the outputted .csv files to a local folder
    save_csv_dpath = expanduser('~/Desktop/.tmp_imessage_extractor') if save_csv is None else save_csv
    if not isdir(save_csv_dpath):
        makedirs(save_csv_dpath)
        logger.info(f'Created temporary export directory {path(save_csv_dpath)}')

    # Extract metadata for each table in chat.db
    chat_db_extract = ChatDbExtract(sqlite_con, save_pg_schema, logger)

    # Save tables to .csv files
    logger.info(f'Saving tables to {path(save_csv_dpath)}')
    chat_db_extract.save_to_csv(save_csv_dpath, logger)

    # Refresh target Postgres schema
    if save_pg_schema is not None:
        # Get Postgres credentials and connect to database
        hostname, port, db_name, pg_user, pw = parse_pg_credentials(pg_credentials)
        pg = pydoni.Postgres(hostname=hostname,
                             port=port,
                             db_name=db_name,
                             pg_user=pg_user,
                             pw=pw)

        logger.info(f'Connected to Postgres database {bold(db_name)} hosted on {bold(hostname + ":" + port)}')

        # Drop all objects in the Postgres schema to rebuild it from scratch
        pg.execute(f'drop schema if exists {save_pg_schema} cascade')
        pg.execute(f'create schema {save_pg_schema}')
        # pg.drop_schema_if_exists_and_recreate(pg_schema, cascade=True)  # TODO: uncomment when new version of pydoni released
        logger.info(f'Re-created schema from scratch')

        logger.info(f'Saving tables to schema {bold(save_pg_schema)}')
        chat_db_extract.save_to_postgres(pg, save_pg_schema, logger)

        # DEFINE MANUAL TABLES

        # Define all Postgres views
        vw_names = list_view_names(vw_dpath)
        with open(join(dirname(__file__), 'view_info.json'), 'r') as f:
            vw_info = json.load(f)

        for vw_name in vw_names:
            if vw_name not in vw_info.keys():
                raise Exception(pydoni.advanced_strip(f"""View definition {bold(vw_name)}
                found at {path(join(vw_dpath, vw_name + ".sql"))} but not accounted
                for in {path("view_info.json")}"""))

        for vw_name in vw_info.keys():
            if vw_name not in vw_names:
                raise Exception(pydoni.advanced_strip(f"""View definition {bold(vw_name)}
                found in {path("view_info.json")} but not accounted
                for at {path(join(vw_dpath, vw_name + ".sql"))}"""))

        vw_objects = []
        for vw_name in vw_names:
            vw_objects.append(View(vw_name=vw_name,
                                   vw_dpath=vw_dpath,
                                   reference=vw_info[vw_name]['reference'],
                                   pg_schema=save_pg_schema,
                                   pg=pg,
                                   logger=logger))

    else:
        logger.info('User opted not to save tables to a Postgres database')



    if save_csv is None:
        shutil.rmtree(save_csv_dpath)
        logger.info(f'Removed {path(save_csv_dpath)}')
