import click
import logging
import pandas as pd
import json
import re
import sqlite3
from pydoni import Postgres
from os.path import join, dirname, isfile
from os import stat
import pydoni
from .verbosity import bold, path
import typing


table_info_json_fpath = join(dirname(__file__), 'table_info.json')


class ChatDbTable(object):
    """
    Store metadata from a single table in chat.db.
    """
    def __init__(self,
                 sqlite_con: sqlite3.Connection,
                 table_name: str,
                 write_mode: str,
                 primary_key: typing.Union[str, list],
                 pg_schema: str) -> None:
        self.sqlite_con = sqlite_con
        self.sqlite_cursor = self.sqlite_con.cursor()
        self.table_name = table_name
        self.write_mode = write_mode
        self.primary_key = primary_key
        self.pg_schema = pg_schema
        self.shape = self.get_shape()
        self.create_sql = self.get_create_sql()
        self.csv_fpath = None

    def get_create_sql(self) -> str:
        """
        Query SQLite for the table creation statement.
        """
        # Get create SQL in SQLite dialect
        query_sql = f"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{self.table_name}'"
        self.sqlite_cursor.execute(query_sql)
        create_sql_str = [x[0] for x in self.sqlite_cursor.fetchall()][0]

        # Translate SQLite format to Postgres format
        create_sql_str = create_sql_str.replace('BLOB', 'TEXT')
        create_sql_str = create_sql_str.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY')

        # Add schema name, and quote both the schema name and table name
        create_sql_str = re.sub(r'(CREATE TABLE) (.*?) ', fr'\1 "{self.pg_schema}"."\2" ', create_sql_str)

        return create_sql_str

    def get_shape(self) -> tuple:
        """
        Query SQLite for the table's shape in format `(rows, columns)`.
        """
        self.sqlite_cursor.execute(f'PRAGMA table_info({self.table_name})')
        n_cols = len(self.sqlite_cursor.fetchall())

        self.sqlite_cursor.execute(f'SELECT COUNT(*) FROM {self.table_name}')
        n_rows = self.sqlite_cursor.fetchall()[0][0]

        return (n_rows, n_cols)

    def save_to_csv(self, file_name: str) -> None:
        """
        Save table to a .csv file.
        """
        df = pd.read_sql(f'SELECT * FROM {self.table_name}', self.sqlite_con)
        df.to_csv(file_name, index=False)
        self.csv_fpath = file_name

    def save_to_postgres(self, pg: Postgres, pg_schema: str) -> None:
        """
        Save table to Postgres.
        """
        assert isfile(self.csv_fpath), \
            f'Must create {path(self.csv_fpath)} before inserting to Postgres table {bold(self.table_name)}'

        # Create table
        pg.drop_table_if_exists(pg_schema, self.table_name)
        pg.execute(self.create_sql)

        # Save table to Postgres
        load_sql = pydoni.advanced_strip(f"""
        COPY {pg_schema}.{self.table_name}
        FROM '{self.csv_fpath}'
        (DELIMITER ',',
        FORMAT CSV,
        HEADER)
        """)
        pg.execute(load_sql)


class ChatDbExtract(object):
    """
    Store dictionary of `ChatDbTable` objects.
    """
    def __init__(self, sqlite_con, pg_schema: str, logger: logging.Logger) -> None:
        self.sqlite_con = sqlite_con
        self.pg_schema = pg_schema
        self.logger = logger
        self.table_objects = self.extract()

    def extract(self) -> dict:
        """
        Query SQLite database for iMessage tables
        """
        sqlite_cursor = self.sqlite_con.cursor()

        # Get full list of SQLite tables in DB
        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
        sqlite_table_names = [x[0] for x in sqlite_cursor.fetchall()]

        # Read table data stored as local JSON
        with open(table_info_json_fpath) as f:
            table_info = json.load(f)

        # Cross reference queried SQLite tables with those in table_info.json. If a table is
        # found in SQLite but not in table_info.json or the reverse, then an error is thrown.

        for table_name in sqlite_table_names:
            if table_name not in table_info.keys():
                raise Exception(f'Table {bold(table_name)} found in SQLite but not accounted for in {path("table_info.json")}')

        for table_name in table_info.keys():
            if table_name not in sqlite_table_names:
                raise Exception(f'Table {bold(table_name)} found in {path("table_info.json")} but not in SQLite')

        # Extract all rows for all tables in chat.db
        self.logger.info('Reading chat.db source table metadata')
        table_objects = {}
        for table_name, table_data in table_info.items():
            table_object = ChatDbTable(sqlite_con=self.sqlite_con,
                                       table_name=table_name,
                                       write_mode=table_data['write_mode'],
                                       primary_key=table_data['primary_key'],
                                       pg_schema=self.pg_schema)

            table_objects[table_name] = table_object
            self.logger.info(pydoni.advanced_strip(f"""
            Read SQLite:{click.style(table_name, bold=True)},
            shape: {table_object.shape},
            primary key: {str(table_data['primary_key'])}"""), arrow='white')

        return table_objects

    def save_to_csv(self, dir_name: str, logger: logging.Logger) -> None:
        """
        Save all tables to .csv files.
        """
        for table_name, table_object in self.table_objects.items():
            ext = '.csv'
            output_file_name = join(dir_name, table_name + ext)
            table_object.save_to_csv(output_file_name)
            file_size_str = pydoni.human_filesize(stat(output_file_name).st_size)
            logger.info(f'Saved table {bold(table_name)} to {bold(table_name + ext)} ({file_size_str})', arrow='white')

    def save_to_postgres(self, pg: Postgres, pg_schema: str, logger: logging.Logger) -> None:
        """
        Save all tables to Postgres.
        """
        for table_name, table_object in self.table_objects.items():
            table_object.save_to_postgres(pg=pg, pg_schema=pg_schema)
            logger.info(f'Saved Postgres:{bold(self.pg_schema + "." + table_name)}', arrow='white')
