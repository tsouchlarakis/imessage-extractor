import click
import pandas as pd
import sqlite3


class ChatDbTable(object):
    """
    Store data from a single table in chat.db.
    """
    def __init__(self, df: pd.DataFrame, sqlite_table_name: str) -> None:
        self.df = df
        self.shape = df.shape
        self.sqlite_table_name = sqlite_table_name


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


def extract_sqlite(logger, sqlite_con: sqlite3.Connection) -> ChatDbExtract:
    """
    Query SQLite database for iMessage tables, and filter for records not already in
    the Postgres database mirrored tables.

    For example if this workflow was run 1hr ago, only extract the last hour's worth of
    iMessage data from SQLite, rather than for all time.
    """
    logger.info(f"->->->->->->->->->->->->-> {click.style('Extract', bold=True)} <-<-<-<-<-<-<-<-<-<-<-<-<-")

    # Connect to SQLite chat.db
    sqlite_cursor = sqlite_con.cursor()

    # Get full list of SQLite tables in DB
    sqlite_cursor.execute("select name from sqlite_master where type = 'table';")
    sqlite_tables = [x[0] for x in sqlite_cursor.fetchall()]

    chat_db_extract = ChatDbExtract()

    # Extract all rows for all tables in chat.db
    logger.info('Reading chat.db source tables...', bold=True)
    for tname in [x for x in sqlite_tables]:
        df_sqlite = pd.read_sql(f'select * from {tname}', sqlite_con)
        tobject = ChatDbTable(df_sqlite, sqlite_table_name=tname)
        chat_db_extract.add_table(tname, tobject)
        logger.info(f"Read SQLite:{click.style(tname, bold=True)}, shape: {df_sqlite.shape}", arrow='white')

    logger.info('iMessage data successfully extracted from chat.db ✔️', bold=True)
    return chat_db_extract
