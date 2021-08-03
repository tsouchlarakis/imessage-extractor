import sqlite3
import click
import shutil
import time
import pydoni
import logging
from .verbosity import print_startup_message, logger_setup, path, bold
from os import makedirs
from os.path import expanduser, isfile, isdir
from .objects import ChatDbExtract
from .save_to_postgres import save_to_postgres


logger = logger_setup(name='imessage-extractor', level=logging.ERROR)


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
@click.option('--rebuild', is_flag=True, default=False,
              help='If saving to Postgres, fully rebuild tables from chat.db source.')

@click.command()
def go(chat_db_path,
       verbose,
       save_csv,
       save_pg_schema,
       pg_credentials,
       rebuild):
    """
    Run the imessage-extractor.
    """
    # Configure logger
    logging_level = logging.INFO if verbose else logging.ERROR
    logger = logger_setup(name='imessage-extractor', level=logging_level)

    # Validate parameters
    if save_csv is None:
        if save_pg_schema is None:
            raise ValueError('Must specify either --save-csv or --save-pg-schema')

    if (save_pg_schema is not None and pg_credentials is None) \
        or (save_pg_schema is None and pg_credentials is not None):
            raise ValueError('Must specify both --pg-credentials and --save-pg-schema if one is specified')

    # Log parameter values
    params = {k: v for k, v in locals().items() if k not in ['logger', 'logging_level']}
    for name, value in params.items():
        logger.debug(f'{name}: {value}')

    # Begin pipeline stopwatch
    start_ts = time.time()

    if verbose:
        print_startup_message(logger)

    # logger.info(f"->->->->->->->->->->->->-> {click.style('Extract', bold=True)} <-<-<-<-<-<-<-<-<-<-<-<-<-")

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

    # Save tables to Postgres
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
                             pg_user=user,
                             pw=pw)

        logger.info(f'Connected to Postgres database {bold(db_name)} hosted on {bold(hostname + ":" + port)}')

        logger.info(f'Saving tables to schema {bold(save_pg_schema)}')
        chat_db_extract.save_to_postgres(pg, save_pg_schema, logger)

        # save_to_postgres(pg=pg,
        #                  chat_db_extract=chat_db_extract,
        #                  pg_schema=save_pg_schema,
        #                  rebuild=rebuild,
        #                  logger=logger)

    else:
        logger.info('User opted not to save tables to a Postgres database')



    # logger.info(f"->->->->->->->->->->->->-> {click.style('Save', bold=True)} <-<-<-<-<-<-<-<-<-<-<-<-<-")

    # Save tables to local CSV folder. This must be done regardless of the user's preference
    # on whether to finally save the data to CSV files or not, since this is an intermediate
    # step between the SQLite extraction and saving to Postgres.
    # save_csv = expanduser(save_csv)
    # if not isdir(save_csv):
    #     makedirs(save_csv)

    # logger.info(f'Saving tables as CSV files at path {path(save_csv)}')
    # chat_db_extract.save_to_csv(dir_name=save_csv, verbose=verbose, logger=logger)




    if save_csv is None:
        shutil.rmtree(save_csv_dpath)
        logger.info(f'Removed {path(save_csv_dpath)}')
