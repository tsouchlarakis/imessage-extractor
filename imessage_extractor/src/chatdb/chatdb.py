import json
import logging
import pandas as pd
import pathlib
import re
import sqlite3
import typing
from ..helpers.config import WorkflowConfig
from ..helpers.verbosity import bold, path, code
from os.path import isfile, join, expanduser
from pydoni import advanced_strip, Postgres, ensurelist


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
        advanced_strip("""Invalid structure of supplied Postgres credentials.
        Must be either a path to a local Postgres credentials file 'i.e. ~/.pgpass', OR
        a string with the connection credentials. Must be in format
        'hostname:port:db_name:user_name:password'.""")

    hostname, port, db_name, pg_user, pw = pg_cred_str.split(':')
    return hostname, port, db_name, pg_user, pw


class ChatDb(object):
    """
    Manage database connection to SQLite chat.db.
    """
    def __init__(self, chat_db_path: str, logger: logging.Logger) -> None:
        self.chat_db_path = expanduser(chat_db_path)
        if not isfile(self.chat_db_path):
            raise FileNotFoundError(advanced_strip(
                f"""Chat.db not found at {path(self.chat_db_path)}"""))

        self.logger = logger
        self.sqlite_con = self.connect()

    def connect(self) -> sqlite3.Connection:
        """
        Establish connection to SQLite chat.db.
        """
        try:
            sqlite_con = sqlite3.connect(self.chat_db_path)
            self.logger.info(f'Connected to chat.db {path(self.chat_db_path)}')
            return sqlite_con
        except Exception as e:
            raise(Exception(advanced_strip("""Unable to connect to SQLite! Could it be
            that the executing environment does not have proper permissions? Perhaps wrapping
            the command in an (Automator) application or script, and granting Full Disk
            Access to that application or script might be a potential option""")))

    def disconnect(self) -> None:
        """
        Close connection to SQLite chat.db.
        """
        self.conn.close()
        self.logger.info('Closed connection to chat.db')


class ChatDbTable(object):
    """
    Store metadata from a single table in chat.db.
    """
    def __init__(self,
                 table_name: str,
                 logger: logging.Logger,
                 cfg: 'WorkflowConfig') -> None:
        self.table_name = table_name
        self.logger = logger
        self.cfg = cfg

        with open(self.cfg.file.chatdb_table_info, 'r') as json_file:
            table_info = json.load(json_file)[self.table_name]

        self.__dict__.update(table_info)

        if self.primary_key is not None:
            self.primary_key = ensurelist(self.primary_key)

    def get_shape(self, sqlite_con: sqlite3.Connection) -> None:
        """
        Query SQLite for the table's shape in format `(rows, columns)`.
        """
        cursor = sqlite_con.cursor()

        cursor.execute(f'PRAGMA table_info({self.table_name})')
        n_cols = len(cursor.fetchall())

        cursor.execute(f'SELECT COUNT(*) FROM {self.table_name}')
        n_rows = cursor.fetchall()[0][0]

        self.shape = (n_rows, n_cols)

    def build_pg_create_table_sql(self, sqlite_con: sqlite3.Connection) -> None:
        """
        Transform a string result of an SQL create statement queried from the 'sql' column
        in sqlite_master to a Postgres-interpretable create string, with any other custom
        modifications applied to it.
        """
        def _map_sqlite_dtypes_to_postgres(dtype: str) -> str:
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

        def _apply_table_specific_dtype_mods(table_name: str, column_defs_lst: list) -> list:
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
            if table_name in new_column_datatypes.keys():
                mapping = new_column_datatypes[table_name]
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

        def _add_schema_and_quotes_to_reference(column_defs_lst: list, pg_schema: str) -> list:
            """
            Make sure all tables named in the REFERENCE portion of a columns's datatype string
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

        def _construct(sqlite_create_str: str,
                       pg_schema: str,
                       table_name: str,
                       if_not_exists: bool=False,
                       quote_columns: bool=True) -> str:
            """
            Construct a Postgres-readable create string from the sqlite_create_str.
            """
            if_not_exists_str = 'IF NOT EXISTS ' if if_not_exists else ''

            template = '\n'.join([
                'CREATE TABLE {if_not_exists_str}"{pg_schema}"."{table_name}"',
                '(',
                '\t{column_defs_str}',
                '\t{constraint_str}',
                ');'
            ])

            # Remove CREATE TABLE {table_name} to get just the column definition part of the string
            column_defs = sqlite_create_str.replace('CREATE TABLE', '').replace(table_name, '').strip()

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
                    f'Invalid `name_dtype_str` "{name_dtype_str}" in column specification for table {table_name}'

                name = name_dtype_tuple[0]
                dtype = name_dtype_tuple[1] if len(name_dtype_tuple) == 2 else ''
                dtype = dtype.replace('(ROWID)', '("ROWID")')
                dtype = _map_sqlite_dtypes_to_postgres(dtype)

                column_defs_lst.append((name, dtype))

            column_defs_lst = _apply_table_specific_dtype_mods(table_name, column_defs_lst)
            column_defs_lst = _add_schema_and_quotes_to_reference(column_defs_lst, pg_schema)

            column_defs_lst_quoted = []
            for name, dtype in column_defs_lst:
                if quote_columns:
                    if re.match(rgx_is_column, name):
                        name = f'"{name}"'  # Add quotes around column name

                column_defs_lst_quoted.append((name, dtype))

            column_defs_str = ',\n\t'.join([f'\t{name} {dtype}'.strip() for name, dtype in column_defs_lst_quoted])
            column_defs_str = column_defs_str + ',' if constraint_str > '' else column_defs_str

            assert len(column_defs_str) > 0, f'Unexpected error defining `column_defs_str`, value: "{column_defs_str}"'

            sql = template.format(
                if_not_exists_str=if_not_exists_str,
                pg_schema=pg_schema,
                table_name=table_name,
                column_defs_str=column_defs_str,
                constraint_str=constraint_str
            )

            return sql.replace('\n\n', '\n')


        cursor = sqlite_con.cursor()

        # Get create SQL in SQLite dialect
        query_sql = f"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{self.table_name}'"
        cursor.execute(query_sql)
        sqlite_create_str = [x[0] for x in cursor.fetchall()][0]

        if not len(sqlite_create_str):
            raise ValueError(f'Unexected `sqlite_create_str` value "{sqlite_create_str}"')

        sql = _construct(sqlite_create_str=sqlite_create_str,
                         pg_schema=self.cfg.pg_schema,
                         table_name=self.table_name,
                         if_not_exists=True)

        self.create_sql = sql

    def query_primary_key_max_pg_values(self, pg: Postgres) -> dict:
        """
        Get the maximum value of a numeric, auto-incrementing primary key column (i.e. ROWID).
        This will allow us to only query SQLite for records with primary key values greater
        (more recent) than the highest that currently exists in Postgres. Of course, this will
        only be called if saving data to Postgres, and if `rebuild` is False.

        Return a dictionary with a single value for every primary key column. Will return an
        empty dictionary if:

            1. We're not saving to Postgres
            2. There are no primary key columns specified in chat_db_info.json
            3. The table does not exist in Postgres
            4. There are no records in the Postgres table

        If a dictionary is returned, then maximum primary key value(s) must have been found.
        """
        if self.cfg.pg_schema is None:
            return {}

        if self.primary_key is None:
            return {}

        if not pg.table_exists(self.cfg.pg_schema, self.table_name):
            return {}

        if pg.read_sql(f'select count(*) from "{self.cfg.pg_schema}"."{self.table_name}"').squeeze() == 0:
            return {}

        max_value_dct = {}

        for col in self.primary_key:
            if 'int' in pg.col_dtypes(self.cfg.pg_schema, self.table_name)[col]:
                # Primary key column is an integer column, so query the maximum value
                max_val = pg.read_sql(f'select max("{col}") from {self.cfg.pg_schema}.{self.table_name}')
                max_val = max_val.squeeze()
                max_value_dct[col] = max_val

        return max_value_dct

    def save_to_csv(self, output_fpath: str, sqlite_con: sqlite3.Connection, max_pg_pkey_values: dict=None) -> None:
        """
        Save table to a .csv file.
        """
        self.logger.debug(f'Saving table {bold(self.table_name)} to {path(output_fpath)}')

        where_clause = ''
        if not self.cfg.rebuild:
            # If we're not re-building the table and saving to Postgres then we'd like to
            # query the maximum value of each primary key column in Postgres
            if isinstance(max_pg_pkey_values, dict):
                max_pg_pkey_values = {k: v for k, v in max_pg_pkey_values.items() if v is not None}
                if len(max_pg_pkey_values) > 0:
                    # There must be at least primary key column that we'd like to filter
                    # our query against the SQLite table on
                    and_cond = ' AND '.join([f'{k} > {v}' for k, v in max_pg_pkey_values.items()])
                    where_clause = 'WHERE' + ' ' + and_cond

        if where_clause == '':
            self.logger.debug(f'Querying full table')

        df = pd.read_sql(f'SELECT * FROM {self.table_name} {where_clause}'.strip(), sqlite_con)

        df.to_csv(output_fpath, index=False)

        self.logger.debug(f'Saved dataframe to disk, shape {df.shape}')
        self.csv_fpath = output_fpath

    def save_to_postgres(self, pg: Postgres, pg_schema: str) -> None:
        """
        Save table to Postgres.
        """
        self.logger.debug(f'Saving table {bold(self.table_name)} to Postgres')

        if self.csv_fpath is None:
            raise FileNotFoundError(advanced_strip(f"""
            Must create {path(self.csv_fpath)} before inserting to Postgres
            table {bold(self.table_name)} (hint: call `save_to_csv()` first)"""))
        else:
            assert isfile(self.csv_fpath), f'Expected file {path(self.csv)} does not exist'

        if self.write_mode == 'replace':
            # Ensure that table is empty before copying. This logic can be executed
            # with or without `rebuild`, so the user can control which tables are fully
            # rebuilt with each run by editing the `write_mode` in chatdb_table_info.json.
            pg.drop_table(pg_schema, self.table_name, if_exists=True, cascade=True)
            self.logger.debug(f"""Table has {code('write_mode="replace"')}, so dropped table""")

        if not pg.table_exists(pg_schema, self.table_name):
            pg.execute(self.create_sql)
            self.logger.debug(f'Re-created empty table')

        # Save table to Postgres
        load_sql = advanced_strip(f"""
        copy "{pg_schema}"."{self.table_name}"
        from '{self.csv_fpath}' (
            delimiter ',',
            format csv,
            header
        )""")
        self.logger.debug(code(load_sql))
        pg.execute(load_sql)


class View(object):
    """
    Store information and operations on a target Postgres iMessage database view.
    """
    def __init__(self,
                 vw_name: str,
                 vw_type: str,
                 logger: logging.Logger,
                 cfg: 'WorkflowConfig') -> None:
        self.vw_name = vw_name
        self.vw_type = vw_type
        self.logger = logger
        self.cfg = cfg

        if vw_type == 'chatdb':
            self.def_fpath = join(self.cfg.dir.chatdb_views, self.vw_name + '.sql')
            with open(self.cfg.file.chatdb_view_info, 'r') as f:
                vw_info = json.load(f)

        elif vw_type == 'staging':
            self.def_fpath = join(self.cfg.dir.staging_views, self.vw_name + '.sql')
            with open(self.cfg.file.staging_view_info, 'r') as f:
                vw_info = json.load(f)

        else:
            raise ValueError(f'Unknown view type "{vw_type}". Must be one of "chatdb" or "staging".')

        self.def_sql = self.read_def_sql(self.def_fpath)

        if self.vw_name not in vw_info:
            raise KeyError(f'View "{self.vw_name}" not found in "{self.cfg.file.chatdb_vw_info}"')

        self.vw_info = vw_info[self.vw_name]

        self.references = self.vw_info['reference']
        if not isinstance(self.references, list):
            raise ValueError(advanced_strip(f"References for {bold(self.vw_name)} must be a list"))

        if isinstance(self.references, list) and len(self.references) > 0:
            self.has_references = True
        else:
            self.has_references = False

        self._validate_view_definition()

    def _validate_view_definition(self) -> None:
        """
        Validate that this view has a key: value pair in *_view_info.json and a corresponding
        .sql file.
        """
        if not isfile(self.def_fpath):
            raise FileNotFoundError(advanced_strip(
                f"View definition {bold(self.vw_name)} expected at {path(self.def_fpath)}"))

        if self.vw_type == 'staging':
            vw_info_fpath = self.cfg.file.staging_view_info
        elif self.vw_type == 'chatdb':
            vw_info_fpath = self.cfg.file.chatdb_view_info

        with open(vw_info_fpath, 'r') as f:
            vw_info = json.load(f)

        if self.vw_name not in vw_info:
            raise ValueError(advanced_strip(
                f"""View definition {bold(self.vw_name)} expected as key: value
                pair in {path(vw_info_fpath)}"""))

    def read_def_sql(self, def_fpath: str) -> str:
        """
        Read a .sql file containing a definition of a Postgres view.
        """
        if not isfile(def_fpath):
            raise FileNotFoundError(advanced_strip(
                f"View definition {bold(self.vw_name)} expected at {path(def_fpath)}"))

        try:
            with open(def_fpath, 'r') as sql_file:
                def_sql = sql_file.read().format(pg_schema=self.cfg.pg_schema)
                return def_sql
        except Exception as e:
            raise Exception(advanced_strip(
                f"""View definition exists but {bold(self.vw_name)} is malformed.
                There is one or more format string enclosed in {{}} in the definition
                {path(def_fpath)} that is incompatible with local variables
                stored in each instantiation of the `View` class (stored in
                objects.py). Either modify the class __init__ to contain this
                variable if it is necessary, else remove it from the view
                definition SQL. Original error message: {str(e)}'"""))

    def check_references(self, pg: Postgres) -> None:
        """
        Check whether ALL reference objects (views or tables) for a given view exist, and
        list nonexistent references.
        """
        if self.has_references:
            self.references_exist = all([pg.table_or_view_exists(self.cfg.pg_schema, ref) for ref in self.references])
            self.nonexistent_references = [ref for ref in self.references if not pg.table_or_view_exists(self.cfg.pg_schema, ref)]
        else:
            self.references_exist = True
            self.nonexistent_references = []

    def drop(self, pg: Postgres, if_exists: bool=False, cascade: bool=False) -> None:
        """
        Drop the target view.
        """
        if not hasattr(self, 'references_exist'):
            self.check_references(pg)

        if if_exists:
            if pg.view_exists(self.cfg.pg_schema, self.vw_name):
                pg.drop_view(self.cfg.pg_schema, self.vw_name, cascade=cascade)
                self.logger.info(f'Dropped view "{bold(self.vw_name)}" (cascade {code(cascade)}) ', arrow='red')
        else:
            pg.drop_view(self.cfg.pg_schema, self.vw_name, cascade=cascade)
            self.logger.info(f'Dropped view "{bold(self.vw_name)}" (cascade {code(cascade)}) ', arrow='red')

    def create(self,
               pg: Postgres,
               cascade: bool=False) -> None:
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
            pg.execute(self.def_sql)
        else:
            self.logger.debug(f'Requested cascaded definition for {bold(self.vw_name)}')
            if pg.view_exists(self.cfg.pg_schema, self.vw_name):
                self.logger.debug('View already exists')
            else:
                if not hasattr(self, 'references_exist'):
                    self.check_references(pg)

                if self.references_exist:
                    self.logger.debug('All references exist, creating the view')
                    pg.execute(self.def_sql)
                    self.logger.debug('Defined view successfully')
                    self.logger.info(f'Defined view "{bold(self.vw_name)}"', arrow='cyan')
                else:
                    self.logger.debug(advanced_strip(
                        f"""Cannot create the view because of nonexistent
                        references: {str(self.nonexistent_references)}"""))

                    for ref in self.nonexistent_references:
                        ref_vw_obj = View(vw_name=ref,
                                          vw_type=self.vw_type,
                                          logger=self.logger,
                                          cfg=self.cfg)

                        ref_vw_obj.create(pg=pg, cascade=True)

                    # At this point, we have created all of the refrences for this view,
                    # so we should be able to simply create it as normal
                    pg.execute(self.def_sql)
                    self.logger.debug('Created all nonexistent references and defined view successfully')
                    self.logger.info(f'Defined view "{bold(self.vw_name)}"', arrow='cyan')
