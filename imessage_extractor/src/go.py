import sqlite3
import click
import time
import pydoni
import logging
from .verbosity import print_startup_message, logger_setup, path
from os import makedirs, mkdir
from os.path import expanduser, isfile, isdir
from .extract_sqlite import extract_sqlite


logger = logger_setup(name='imessage-extractor', level=logging.ERROR)


@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
@click.option('--chat-db-path', type=str, default=expanduser('~/Library/Messages/chat.db'),
              help='Path to working chat.db.')
@click.option('--save-csv', type=str, default=None, required=False,
              help='Path to folder to save chat.db tables to')

@click.command()
def go(chat_db_path, verbose, save_csv):
    """
    Run the imessage-extractor.
    """
    # Begin pipeline stopwatch
    start_ts = time.time()

    # Configure logger
    logging_level = logging.INFO if verbose else logging.ERROR
    logger = logger_setup(name='imessage-extractor', level=logging_level)

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
            the command in an application or script, and granting Full Disk Access to that
            application or script might be a potential option""")))
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
        logger.info('Not saving tables as local CSV files')