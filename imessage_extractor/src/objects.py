import click
import logging
import pandas as pd
import sqlite3
from os.path import join
from os import stat
import pydoni
from .verbosity import bold


class ChatDbTable(object):
    """
    Store data from a single table in chat.db.
    """
    def __init__(self, df: pd.DataFrame, sqlite_table_name: str) -> None:
        self.df = df
        self.shape = df.shape
        self.sqlite_table_name = sqlite_table_name

    def save_to_csv(self, file_name: str) -> None:
        """
        Save table to a csv file.
        """
        self.df.to_csv(file_name, index=False)


class ChatDbExtract(object):
    """
    Store dictionary of `ChatDbTable` objects.
    """
    def __init__(self) -> None:
        self.table_objects = {}

    def add_table(self, table_name: str, table_object: ChatDbTable) -> None:
        """
        Append a ChatDbTable object to `self.table_objects`.
        """
        self.table_objects[table_name] = table_object

    def save_to_csv(self, dir_name: str, verbose: bool, logger: logging.Logger) -> None:
        """
        Save all tables to csv files.
        """
        for table_name, table_object in self.table_objects.items():
            ext = '.csv'
            output_file_name = join(dir_name, table_name + ext)
            table_object.save_to_csv(output_file_name)
            if verbose:
                file_size_str = pydoni.human_filesize(stat(output_file_name).st_size)
                logger.info(f'Saved table {bold(table_name)} to {bold(table_name + ext)} ({file_size_str})', arrow='white')


def extract_sqlite(logger, sqlite_con: sqlite3.Connection) -> ChatDbExtract:
    """
    Query SQLite database for iMessage tables, and filter for records not already in
    the Postgres database mirrored tables.

    For example if this workflow was run 1hr ago, only extract the last hour's worth of
    iMessage data from SQLite, rather than for all time.
    """
    # Connect to SQLite chat.db
    sqlite_cursor = sqlite_con.cursor()

    # Get full list of SQLite tables in DB
    sqlite_cursor.execute("select name from sqlite_master where type = 'table';")
    sqlite_tables = [x[0] for x in sqlite_cursor.fetchall()]

    chat_db_extract = ChatDbExtract()

    # Extract all rows for all tables in chat.db
    logger.info('Reading chat.db source tables')
    for tname in [x for x in sqlite_tables]:
        df_sqlite = pd.read_sql(f'select * from {tname}', sqlite_con)
        tobject = ChatDbTable(df_sqlite, sqlite_table_name=tname)
        chat_db_extract.add_table(tname, tobject)
        logger.info(f"Read SQLite:{click.style(tname, bold=True)}, shape: {df_sqlite.shape}", arrow='white')

    logger.info('iMessage data successfully extracted from chat.db ✔️', bold=True)
    return chat_db_extract
