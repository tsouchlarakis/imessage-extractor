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


class CreateTableSQL(object):
    """
    Transform a string result of an SQL create statement queried from the 'sql' column
    in sqlite_master to a Postgres-interpretable create string, with any other custom
    modifications applied to it.
    """
    def __init__(self, sqlite_create_str: str, pg_schema: str, if_not_exists: bool=False) -> None:
        self.sqlite_create_str = sqlite_create_str
        self.table_name = re.search(r'CREATE TABLE (\w+)', sqlite_create_str).group(1)
        self.sql = self._construct(pg_schema, if_not_exists)

    def _map_sqlite_dtypes_to_postgres(self, dtype: str) -> str:
        """
        Translate datatypes only available in SQLite to the closest datatypes in Postgres.
        """
        dtype_map = {
            'BLOB': 'TEXT',
            'INTEGER PRIMARY KEY AUTOINCREMENT': 'SERIAL PRIMARY KEY',
        }

        for invalid_dtype, correct_dtype in dtype_map.items():
            if invalid_dtype in dtype:
                dtype = dtype.replace(invalid_dtype, correct_dtype)

        return dtype

    def _apply_table_specific_dtype_mods(self, column_defs_lst: list) -> list:
        """
        Apply any custom datatype modifications to targeted columns. Modifications are
        defined in the variable `table_specific_mods` in format:

        table_name: {column_name: new_dtype, ...}

        For example, this function may be used to map a specific column from INTEGER to
        BIGINT, since the size constraings on SQLite's INTEGER are different than those
        implemented by Postgres.
        """
        new_column_datatypes = {
            'chat': {
                'last_read_message_timestamp': 'BIGINT',
            },
            'message': {
                'date': 'BIGINT',
                'date_read': 'BIGINT',
                'date_delivered': 'BIGINT',
                'time_expressive_send_played': 'BIGINT',
                'date_played': 'BIGINT',
            },
            'sqlite_sequence': {
                'name': 'TEXT',
                'seq': 'INTEGER',
            },
            'sqlite_stat1': {
                'tbl': 'TEXT',
                'idx': 'TEXT',
                'stat': 'TEXT',
            },
            'chat_message_join': {
                'message_date': 'BIGINT',
            }
        }

        new_column_defs_lst = []
        if self.table_name in new_column_datatypes.keys():
            mapping = new_column_datatypes[self.table_name]
            for column_name, dtype in column_defs_lst:
                if column_name in mapping.keys():
                    # Change the datatype of the column
                    new_dtype = mapping[column_name]
                    new_column_defs_lst.append((column_name, new_dtype))
                else:
                    new_column_defs_lst.append((column_name, dtype))
        else:
            # No changes to apply
            new_column_defs_lst = column_defs_lst

        return new_column_defs_lst

    def _add_schema_and_quotes_to_reference(self, column_defs_lst: list, pg_schema: str) -> list:
        """
        Make sure all tables named in the REFERENCES portion of a columns's datatype string
        have the schema attached. In SQLite, all tables are referenced simply by their name
        but in Postgres we're loading data into a particular schema.
        """
        new_column_defs_lst = []
        for column_name, dtype in column_defs_lst:
            if 'REFERENCES' in dtype:
                # Add the schema to the REFERENCES portion of the datatype string
                dtype = re.sub(r'(REFERENCES) (\w+)', r'\1 "%s"."\2"' % pg_schema, dtype)

            new_column_defs_lst.append((column_name, dtype))

        return new_column_defs_lst

    def _construct(self, pg_schema: str, if_not_exists: bool=False, quote_columns: bool=True) -> str:
        """
        Construct a Postgres-readable create string from the sqlite_create_str.
        """
        if_not_exists_str = 'IF NOT EXISTS ' if if_not_exists else ''

        template = '\n'.join([
            'CREATE TABLE "{if_not_exists_str}{pg_schema}"."{table_name}"',
            '(',
            '\t{column_defs_str}',
            '\t{constraint_str}',
            ');'
        ])

        # Remove CREATE TABLE {table_name} to get just the column definition part of the string
        column_defs = self.sqlite_create_str.replace('CREATE TABLE', '').replace(self.table_name, '').strip()

        # Remove enclosing parentheses
        column_defs = column_defs[1:-1]

        # Separate out constraint strings from column definitions. For example, if a column
        # definition string is:
        #
        # """
        # chat_id INTEGER REFERENCES chat (ROWID) ON DELETE CASCADE,
        # handle_id INTEGER REFERENCES handle (ROWID) ON DELETE CASCADE,
        # UNIQUE(chat_id, handle_id)
        # """
        #
        # Then the UNIQUE... portion is a constraint string, and the preceding portion is
        # the column definition string.
        # constraint_lst = ['UNIQUE', 'PRIMARY KEY']
        constraint_lst = ['UNIQUE', 'PRIMARY KEY']
        for item in constraint_lst:
            column_defs = column_defs.replace(item + ' (', item + '(')

        constraint_lst = [x + '(' for x in constraint_lst]
        constraint_loc = [column_defs.find(x) for x in constraint_lst]
        if any(x > 0 for x in constraint_loc):
            # Remove any constraints from the column definition string
            constraint_loc = [x for x in constraint_loc if x > 0]
            constraint_str = column_defs[min(constraint_loc):].strip()
            column_defs = column_defs[:min(constraint_loc)].strip(' ,')
        else:
            constraint_str = ''

        # Now it's acceptable to split on commas, since we've removed constraints
        column_defs = [x.strip() for x in column_defs.split(',')]

        # Separate column names from datatypes
        rgx_is_column = r'^[A-Za-z_]+$'
        column_defs_lst = []
        for name_dtype_str in column_defs:
            name_dtype_tuple = tuple(name_dtype_str.split(' ', 1))
            assert len(name_dtype_tuple) in [1, 2], \
                f'Invalid `name_dtype_str` "{name_dtype_str}" in column specification for table {self.table_name}'

            name = name_dtype_tuple[0]
            dtype = name_dtype_tuple[1] if len(name_dtype_tuple) == 2 else ''
            dtype = dtype.replace('(ROWID)', '("ROWID")')
            dtype = self._map_sqlite_dtypes_to_postgres(dtype)

            column_defs_lst.append((name, dtype))

        column_defs_lst = self._apply_table_specific_dtype_mods(column_defs_lst)
        column_defs_lst = self._add_schema_and_quotes_to_reference(column_defs_lst, pg_schema)

        column_defs_lst_quoted = []
        for name, dtype in column_defs_lst:
            if quote_columns:
                if re.match(rgx_is_column, name):
                    name = f'"{name}"'  # Add quotes around column name

            column_defs_lst_quoted.append((name, dtype))

        column_defs_str = ',\n\t'.join([f'\t{name} {dtype}'.strip() for name, dtype in column_defs_lst_quoted])
        column_defs_str = column_defs_str + ',' if constraint_str > '' else column_defs_str

        sql = template.format(
            if_not_exists_str=if_not_exists_str,
            pg_schema=pg_schema,
            table_name=self.table_name,
            column_defs_str=column_defs_str,
            constraint_str=constraint_str
        )

        return sql.replace('\n\n', '\n')


class ChatDbTable(object):
    """
    Store metadata from a single table in chat.db.
    """
    def __init__(self,
                 sqlite_con: sqlite3.Connection,
                 table_name: str,
                 write_mode: str,
                 primary_key: typing.Union[str, list],
                 pg_schema: str,
                 references: typing.Union[None, list]) -> None:
        self.sqlite_con = sqlite_con
        self.sqlite_cursor = self.sqlite_con.cursor()
        self.table_name = table_name
        self.write_mode = write_mode
        self.primary_key = primary_key
        self.pg_schema = pg_schema
        self.references = references
        self.shape = self._get_shape()
        self.create_sql = self._get_create_sql()
        self.csv_fpath = None

    def _get_create_sql(self) -> str:
        """
        Query SQLite for the table creation statement.
        """
        # Get create SQL in SQLite dialect
        query_sql = f"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{self.table_name}'"
        self.sqlite_cursor.execute(query_sql)
        create_sql_str = [x[0] for x in self.sqlite_cursor.fetchall()][0]

        create_sql = CreateTableSQL(create_sql_str, self.pg_schema)

        return create_sql.sql

    def _get_shape(self) -> tuple:
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
        COPY "{pg_schema}"."{self.table_name}"
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
        self.table_objects = self._extract()

    def _extract(self) -> dict:
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
                                       references=table_data['references'],
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
        Save all tables to Postgres in such an order that foreign keys are resolved correctly.
        For example, if a table depends on another table, then the other table must be created
        before the dependent table.
        """
        inserted_journal = []  # Keep a log of all tables that have been saved to Postgres

        while len(inserted_journal) < len(self.table_objects):
            for table_name, table_object in self.table_objects.items():
                # if table_name == 'chat_message_join':
                #     import pdb; pdb.set_trace()
                if table_name not in inserted_journal:
                    if table_object.references is not None:
                        if len([t for t in table_object.references if t not in inserted_journal]):
                            # There is still one or more reference table that has not yet
                            # been saved to Postgres. Continue to the next table.
                            continue
                        else:
                            # All necessary reference tables for `table_name` have already
                            # been saved to Postgres, so we can now insert this table
                            table_object.save_to_postgres(pg=pg, pg_schema=pg_schema)
                            inserted_journal.append(table_name)
                            logger.info(f'Saved Postgres:{bold(self.pg_schema + "." + table_name)}', arrow='white')
                    else:
                        # No references found for this table, we can insert it right away
                        # since there are no dependencies to worry about
                        table_object.save_to_postgres(pg=pg, pg_schema=pg_schema)
                        inserted_journal.append(table_name)
                        logger.info(f'Saved Postgres:{bold(self.pg_schema + "." + table_name)}', arrow='white')
                else:
                    # This table has already been saved to Postgres, so we can skip it
                    pass

