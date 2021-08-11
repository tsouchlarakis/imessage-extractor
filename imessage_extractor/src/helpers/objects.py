import click
import json
import logging
import pandas as pd
import pydoni
import re
import sqlite3
import typing
from .verbosity import bold, path
from os import stat
from os.path import join, dirname, isfile, abspath


vw_dpath = abspath(join(dirname(__file__), 'views'))
vw_def_dpath = join(vw_dpath, 'definitions')
staged_table_dpath = abspath(join(dirname(__file__), 'tables', 'staging'))
table_info_json_fpath = abspath(join(dirname(__file__), 'tables', 'chatdb_table_info.json'))
schemas_fpath = join(custom_table_dpath, 'custom_table_schemas.json')


class Attribute():
    pass


class WorkflowConfig(object):
    """
    Store configuration variables for iMessage Extractor workflow.
    """
    def __init__(self) -> None:
        self.dir = Attribute()
        self.dir.home = dirname(__file__)
        self.dir.qc = join(self.dir.home, 'quality_control')
        self.dir.tables = join(self.dir.home, 'quality_control')
        self.dir.qc = join(self.dir.home, 'quality_control')
        self.dir.qc = join(self.dir.home, 'quality_control')

        self.file = Attribute()


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
                 max_pg_primary_key_value: typing.Union[int, tuple, None],
                 pg_schema: str,
                 references: typing.Union[None, list],
                 rebuild: bool,
                 logger: logging.Logger) -> None:
        self.sqlite_con = sqlite_con
        self.sqlite_cursor = self.sqlite_con.cursor()
        self.table_name = table_name
        self.write_mode = write_mode
        self.primary_key = primary_key
        self.max_pg_primary_key_value = max_pg_primary_key_value
        self.pg_schema = pg_schema
        self.references = references
        self.rebuild = rebuild
        self.logger = logger
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
        self.logger.debug(f'Saving table {bold(self.table_name)} to {path(file_name)}')

        where_clause = ''
        if not self.rebuild:
            if self.max_pg_primary_key_value is not None:
                if isinstance(self.primary_key, list):  # Multiple primary keys
                    if len(self.primary_key) <= 1:
                        raise ValueError(pydoni.advanced_strip(
                            f"""Primary keys for table {bold(self.table_name)}
                            are a list but are of length 1. Change the primary key
                            definition in {path('chatdb_table_info.json')} to be a string,
                            rather than a list."""))

                    pkey = list(self.primary_key)
                    max_pkey = list(self.max_pg_primary_key_value)
                else:
                    pkey = pydoni.ensurelist(self.primary_key)
                    max_pkey = pydoni.ensurelist(self.max_pg_primary_key_value)

                assert len(max_pkey) == len(pkey), \
                    f'Values `max_pkey` ({str(max_pkey)}) and `pkey` ({str(pkey)}) are of unequal length'

                where_lst = []
                for k, v in zip(pkey, max_pkey):
                    where_lst.append(f'"{k}" > {v}')
                    self.logger.debug(f'Querying SQLite only above "{k}" > {v}')

                where_clause = 'WHERE ' + ' AND'.join(where_lst)

        if where_clause == '':
            self.logger.debug(f'Querying full table')

        df = pd.read_sql(f'SELECT * FROM {self.table_name} {where_clause}', self.sqlite_con)

        df.to_csv(file_name, index=False)
        self.logger.debug(f'Saved dataframe to disk of shape {bold(df.shape)}')
        self.csv_fpath = file_name

    def save_to_postgres(self, pg: pydoni.Postgres, pg_schema: str) -> None:
        """
        Save table to Postgres.
        """
        self.logger.debug(f'Saving table {bold(self.table_name)} to Postgres')

        if self.csv_fpath is None:
            raise FileNotFoundError(pydoni.advanced_strip(f"""
            Must create {path(self.csv_fpath)} before inserting to Postgres
            table {bold(self.table_name)} (hint: call `save_to_csv()` first)"""))
        else:
            assert isfile(self.csv_fpath), f'Expected file {path(self.csv)} does not exist'

        if self.write_mode == 'replace':
            # Ensure that table is empty before copying. This logic can be executed
            # with or without `rebuild`, so the user can control which tables are fully
            # rebuilt with each run by editing the `write_mode` in chatdb_table_info.json.
            pg.drop_table(pg_schema, self.table_name, if_exists=True, cascade=True)
            self.logger.debug(f'Table has `write_mode="replace"`, so dropped table')

        if not pg.table_exists(pg_schema, self.table_name):
            pg.execute(self.create_sql)
            self.logger.debug(f'Re-created empty table')

        # Save table to Postgres
        load_sql = pydoni.advanced_strip(f"""
        COPY "{pg_schema}"."{self.table_name}"
        FROM '{self.csv_fpath}' (
            DELIMITER ',',
            FORMAT CSV,
            HEADER
        )""")
        self.logger.debug(load_sql)
        pg.execute(load_sql)


class ChatDbExtract(object):
    """
    Store dictionary of `ChatDbTable` objects.
    """
    def __init__(self,
                 sqlite_con: sqlite3.Connection,
                 pg: pydoni.Postgres,
                 pg_schema: str,
                 logger: logging.Logger,
                 rebuild: bool) -> None:
        self.sqlite_con = sqlite_con
        self.pg = pg  # Passed if saving data to Postgres, otherwise None
        self.pg_schema = pg_schema
        self.rebuild = rebuild
        self.logger = logger
        self.table_objects = self._extract()

    def _get_max_pg_primary_key_value(self,
                                      table_name: str,
                                      primary_key: typing.Union[str, list, None]) -> typing.Union[int, tuple, None]:
        """
        Get the maximum value of a numeric, auto-incrementing primary key column (i.e. ROWID).
        This will allow us to only query SQLite for records with primary key values greater
        (more recent) than the highest that currently exists in Postgres. Of course, this will
        only be called if saving data to Postgres, and if `rebuild` is False.
        """
        pkey = pydoni.ensurelist(primary_key)
        max_values = []
        for val in pkey:
            if 'int' in self.pg.col_dtypes(self.pg_schema, table_name)[val]:
                max_val = self.pg.read_sql(f'select max("{val}") from {self.pg_schema}.{table_name}')
                max_val = max_val.squeeze()
                max_values.append(max_val)

        if len(max_values) > 1:
            max_values = tuple(max_values)
        elif len(max_values) == 1:
            max_values = max_values[0]
        else:
            max_values = None

        return max_values

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

        # Cross reference queried SQLite tables with those in chatdb_table_info.json. If a table is
        # found in SQLite but not in chatdb_table_info.json or the reverse, then an error is thrown.

        for table_name in sqlite_table_names:
            if table_name not in table_info.keys():
                raise Exception(f'Table {bold(table_name)} found in SQLite but not accounted for in {path("chatdb_table_info.json")}')

        for table_name in table_info.keys():
            if table_name not in sqlite_table_names:
                raise Exception(f'Table {bold(table_name)} found in {path("chatdb_table_info.json")} but not in SQLite')

        # Extract all rows for all tables in chat.db
        self.logger.info('Reading chat.db source table metadata')
        table_objects = {}
        for table_name, table_data in table_info.items():
            max_pg_pkey_values = None  # Default case
            if not self.rebuild:  # Not rebuilding pipeline
                if self.pg_schema is not None:  # Saving data to Postgres
                    if table_data['primary_key'] is not None:  # Primary key column exists
                        if self.pg.table_exists(self.pg_schema, table_name):  # Table exists in Postgres
                            max_pg_pkey_values = self._get_max_pg_primary_key_value(table_name, table_data['primary_key'])
                            self.logger.debug(f'`max_pg_pkey_values`: {str(max_pg_pkey_values)}')

            assert table_data['write_mode'] in ['replace', 'append']

            table_object = ChatDbTable(sqlite_con=self.sqlite_con,
                                       table_name=table_name,
                                       write_mode=table_data['write_mode'],
                                       primary_key=table_data['primary_key'],
                                       max_pg_primary_key_value=max_pg_pkey_values,
                                       references=table_data['references'],
                                       rebuild=self.rebuild,
                                       logger=self.logger,
                                       pg_schema=self.pg_schema)

            table_objects[table_name] = table_object
            self.logger.info(pydoni.advanced_strip(f"""
            Read SQLite:{click.style(table_name, bold=True)},
            shape: {table_object.shape},
            primary key: {str(table_data['primary_key'])}"""), arrow='black')

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
            logger.info(
                f"""Saved SQLite:{bold(table_name)} to {path(table_name + ext)}
                ({file_size_str}), shape {table_object.shape}
                """, arrow='white')

    def save_to_postgres(self, pg: pydoni.Postgres, pg_schema: str, logger: logging.Logger) -> None:
        """
        Save all tables to Postgres in such an order that foreign keys are resolved correctly.
        For example, if a table depends on another table, then the other table must be created
        before the dependent table.
        """
        inserted_journal = []  # Keep a log of all tables that have been saved to Postgres

        while len(inserted_journal) < len(self.table_objects):
            for table_name, table_object in self.table_objects.items():
                if self.rebuild or table_object.write_mode == 'replace':
                    participle = 'Rebuilt'
                else:
                    participle = 'Refreshed'

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
                            logger.info(f'{participle} Postgres:"{bold(pg_schema)}"."{bold(table_name)}"', arrow='cyan')
                    else:
                        # No references found for this table, we can insert it right away
                        # since there are no dependencies to worry about
                        table_object.save_to_postgres(pg=pg, pg_schema=pg_schema)
                        inserted_journal.append(table_name)
                        logger.info(f'{participle} Postgres:"{bold(pg_schema)}"."{bold(table_name)}"', arrow='cyan')
                else:
                    # This table has already been saved to Postgres, so we can skip it
                    pass


class View(object):
    """
    Store information and operations on a target Postgres iMessage database view.
    """
    def __init__(self,
                 pg_schema: str,
                 vw_name: str,
                 pg: pydoni.Postgres,
                 logger: logging.Logger) -> None:
        self.pg_schema = pg_schema
        self.vw_name = vw_name
        self.pg = pg
        self.logger = logger

        self.fpath = join(vw_dpath, 'definitions', self.vw_name + '.sql')
        if not isfile(self.fpath):
            raise FileNotFoundError(pydoni.advanced_strip(
                f"""View {bold(self.vw_name)} definition expected at {path(self.fpath)}
                but not found"""))

        try:
            with open(self.fpath, 'r') as f:
                self.def_sql = f.read().format(pg_schema=pg_schema)
        except Exception as e:
            raise Exception(pydoni.advanced_strip(
                f"""View definition exists but {bold(self.vw_name)} is malformed.
                There is one or more format string enclosed in {{}} in the definition
                {path(self.fpath)} that is incompatible with local variables
                stored in each instantiation of the `View` class (stored in
                objects.py). Either modify the class __init__ to contain this
                variable if it is necessary, else remove it from the view
                definition SQL. Original error message: {str(e)}'"""))

        vw_info_chatdb_dependent_fpath = join(vw_dpath, 'view_info_chatdb_dependent.json')
        with open(vw_info_chatdb_dependent_fpath, 'r') as f:
            vw_info_chatdb_dependent = json.load(f)

        vw_info_staging_dependent_fpath = join(vw_dpath, 'view_info_staging_dependent.json')
        with open(vw_info_staging_dependent_fpath, 'r') as f:
            vw_info_staging_dependent = json.load(f)

        if vw_name in vw_info_chatdb_dependent and vw_name in vw_info_staging_dependent:
            raise KeyError(pydoni.advanced_strip(
                f"""View {bold(self.vw_name)} defined in both
                {path(vw_info_chatdb_dependent_fpath)} and {path(vw_info_staging_dependent)}"""))

        self.is_chatdb_dependent = vw_name in vw_info_chatdb_dependent
        self.is_staging_dependent = vw_name in vw_info_staging_dependent

        if not self.is_chatdb_dependent and not self.is_staging_dependent:
            raise KeyError(pydoni.advanced_strip(
                f"""View {bold(self.vw_name)} not defined in either
                {path(vw_info_chatdb_dependent_fpath)} or {path(vw_info_staging_dependent)}"""))

        if self.is_chatdb_dependent:
            self.view_info = vw_info_chatdb_dependent[vw_name]
        elif self.is_staging_dependent:
            self.view_info = vw_info_staging_dependent[vw_name]
        else:
            raise Exception(f'Unexpected error initializing {bold(self.vw_name)}')

        self.references = self.view_info['reference']
        if not isinstance(self.references, list):
            expected_fpath = vw_info_chatdb_dependent_fpath if self.is_chatdb_dependent else vw_info_staging_dependent_fpath
            raise ValueError(f'References for {bold(self.vw_name)} defined in {path(expected_fpath)} must be a list')

        if isinstance(self.references, list) and len(self.references) > 0:
            self.has_references = True
        else:
            self.has_references = False

        self.references_exist = self.check_references()
        self.nonexistent_references = self.list_nonexistent_references()

    def check_references(self) -> bool:
        """
        Check whether ALL reference objects (views or tables) for a given view exist.
        """
        if self.has_references:
            return all([self.pg.table_or_view_exists(self.pg_schema, ref) for ref in self.references])
        else:
            return True

    def list_nonexistent_references(self) -> list:
        """
        List the nonexistent references for a given view.
        """
        if self.has_references:
            return [ref for ref in self.references if not self.pg.table_or_view_exists(self.pg_schema, ref)]
        else:
            return []

    def drop(self, if_exists: bool=False, cascade: bool=False) -> None:
        """
        Drop the target view.
        """
        if if_exists:
            if self.pg.view_exists(self.pg_schema, self.vw_name):
                self.pg.drop_view(self.pg_schema, self.vw_name, cascade=cascade)
                self.logger.info(f'Dropped view {bold(self.vw_name)}')
        else:
            self.pg.drop_view(self.pg_schema, self.vw_name, cascade=cascade)
            self.logger.info(f'Dropped view {bold(self.vw_name)}')


    def create(self, cascade: bool=False) -> None:
        """
        Define a view with or without cascade. Views may be dependent on other views or tables,
        and as such, as we cannot simply execute a view definition since a dependency of that
        view might not exist yet. Instead, we will define views in a manner that ensures
        that all dependencies for a particular view are created before executing that view's
        definition.

        We will iterate through each view in the view_info*.json file, and for each view,
        check each of its dependencies (if any) to ensure that they exist. If one or more
        do not, we must navigate to that view in the view_info*.json file and ensure that
        all of that view's dependencies exist. If one or more do not yet exist, we must then
        continue navigating down the tree of dependencies until we can create all of them.

        For example, suppose view A depends on view B and view C, and view B depends on view D.
        We will attempt to create view A, but it depends on two non-existent views, B and C. We
        then navigate to view B and find that it depends on view D. We create view D for which
        all dependencies exist. Then we can create view B. We then check view C, and find that
        we are able to create it without issue. At this point, all dependencies for view A
        exist and we can create view A.
        """
        if not cascade:
            self.pg.execute(self.def_sql)
        else:
            self.logger.debug(f'Requested definition for {bold(self.vw_name)}')
            if self.pg.view_exists(self.pg_schema, self.vw_name):
                self.logger.debug('View already exists')
            else:
                if self.references_exist:
                    self.logger.debug('All references exist, creating the view')
                    self.pg.execute(self.def_sql)
                    self.logger.debug('Defined view successfully')
                    self.logger.info(f'Defined Postgres:"{bold(self.pg_schema)}"."{bold(self.vw_name)}"', arrow='cyan')
                else:
                    self.logger.debug(pydoni.advanced_strip(
                        f"""Cannot create the view because of nonexistent
                        references: {str(self.nonexistent_references)}. Attempting to
                        define them now"""))

                    for ref in self.nonexistent_references:
                        # Recursively create each reference
                        View(pg_schema=self.pg_schema,
                             vw_name=ref,
                             pg=self.pg,
                             logger=self.logger).create(cascade=True)

                    # At this point, we have created all of the refrences for this view,
                    # so we should be able to simply create it as normal
                    self.pg.execute(self.def_sql)
                    self.logger.debug('Created all nonexistent references and defined view successfully')
                    self.logger.info(f'Defined Postgres:"{bold(self.pg_schema)}"."{bold(self.vw_name)}"', arrow='cyan')


class StagingTable(object):
    """
    Store information and operations for tables that get staged after chat.db
    data has been loaded into Postgres.
    """
    def __init__(self,
                 pg: pydoni.Postgres,
                 pg_schema: str,
                 table_name: str,
                 refresh_function: typing.Callable,
                 logger=logging.Logger) -> None:
        self.pg = pg
        self.pg_schema = pg_schema
        self.table_name = table_name
        self.refresh_function = refresh_function
        self.logger = logger

        with open(join(staged_table_dpath, 'staging_table_info.json')) as f:
            json_data = json.load(f)
            if self.table_name in json_data.keys():
                json_data = json_data[self.table_name]
            else:
                raise KeyError(f'Table {table_name} expected as a key in staging_table_info.json but not found')

        self.columnspec = json_data['columnspec']
        self.primary_key = json_data['primary_key']
        self.references = json_data['references']
        self.check_references(references=self.references)

        assert isinstance(self.columnspec, dict), \
            f'Columnspec for {self.table_name} must be a dictionary'
        assert isinstance(self.primary_key, str) or isinstance(self.primary_key, list), \
            f'Primary key for {self.table_name} must be a string or list'
        assert self.references is None or isinstance(self.references, list), \
            f'References for {self.table_name} must be None or a list'

    def check_references(self, references) -> bool:
        """
        Return True if all reference objects exist in Postgres schema, and return
        an error otherwise.
        """
        if isinstance(references, list):
            missing_refs = []
            for ref in references:
                if not self.pg.table_or_view_exists(self.pg_schema, ref):
                    missing_refs.append(ref)

            if len(missing_refs) > 0:
                raise Exception(f'Staging table {bold(self.table_name)} requires the following non-existent references: {str(missing_refs)}')

        return True

    def refresh(self):
        """
        Execute custom refresh function for a particular table. Refresh functions are
        stored as python modules and live in relative directory refresh_functions/
        """
        self.refresh_function(pg=self.pg,
                              pg_schema=self.pg_schema,
                              table_name=self.table_name,
                              columnspec=self.columnspec,
                              logger=self.logger)


class CustomTable(object):
    """
    Store information and operations on user-defined custom tables.
    """
    def __init__(self,
                 pg_schema: str,
                 table_name: str,
                 pg: pydoni.Postgres,
                 logger: logging.Logger) -> None:
        self.pg_schema = pg_schema
        self.table_name = table_name
        self.pg = pg
        self.logger = logger



