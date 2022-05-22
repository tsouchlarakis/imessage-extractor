import json
import logging
import pandas as pd
import sqlite3
import subprocess
from imessage_extractor.src.helpers.config import WorkflowConfig
from imessage_extractor.src.helpers.utils import strip_ws
from imessage_extractor.src.helpers.verbosity import bold, path, code
from os import mkdir, remove
from os.path import isfile, join, expanduser, isdir, dirname


sqlite_failed_connection_string = strip_ws("""Unable to connect to SQLite! Could it be
    that the executing environment does not have proper permissions? Perhaps wrapping
    the command in an (Automator) application or script, and granting Full Disk
    Access to that application or script might be a potential option""")


class SQLiteDb(object):
    """
    Manage database connection to SQLite chat.db.

    'native' refers to the original chat.db on the macOS system at ~/Library/Messages
    'copied' refers to the copied chat.db to a separate directory on system to prevent damage

    This object first copies the native chat.db to the specified copied directory path location,
    then all subsequent actions on chat.db will point to the copied chat.db rather than the
    native one.
    """
    def __init__(self, db_path: str, logger: logging.Logger) -> None:
        self.logger = logger
        self.db_path = db_path
        self.sqlite_con = None
        self.sqlite_failed_connection_string = sqlite_failed_connection_string

    def connect(self) -> sqlite3.Connection:
        """
        Establish connection to SQLite chat.db.
        """
        try:
            sqlite_con = sqlite3.connect(self.db_path)
            return sqlite_con
        except Exception as e:
            raise Exception(self.sqlite_failed_connection_string)

    def execute(self, sql: str) -> None:
        """
        Execute SQL.
        """
        cursor = self.sqlite_con.cursor()
        cursor.executescript(sql)
        self.sqlite_con.commit()

    def list_tables(self) -> list:
        """
        List tables present in chat.db.
        """
        cursor = self.sqlite_con.cursor()
        return [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

    def table_exists(self, table_name: str) -> bool:
        """
        Indicate whether a table exists.
        """
        cursor = self.sqlite_con.cursor()
        return cursor.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table_name}';").fetchone()[0] == 1

    def list_views(self) -> list:
        """
        List views present in chat.db.
        """
        cursor = self.sqlite_con.cursor()
        return [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall()]

    def view_exists(self, view_name: str) -> bool:
        """
        Indicate whether a table exists.
        """
        cursor = self.sqlite_con.cursor()
        return cursor.execute(f"SELECT count(*) FROM sqlite_master WHERE type='view' AND name='{view_name}';").fetchone()[0] == 1

    def drop_view(self, view_name: str) -> None:
        """
        Drop a view.
        """
        cursor = self.sqlite_con.cursor()
        cursor.execute(f'DROP VIEW IF EXISTS `{view_name}`;')
        self.sqlite_con.commit()

    def table_or_view_exists(self, table_or_view_name: str) -> bool:
        """
        Indicate whether a table or view exists.
        """
        return table_or_view_name in self.list_tables() or table_or_view_name in self.list_views()

    def list_triggers(self) -> list:
        """
        List all triggers in SQLite database.
        """
        cursor = self.sqlite_con.cursor()
        return [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger';").fetchall()]

    def drop_trigger(self, trigger_name: str) -> None:
        """
        Drop a SQLite trigger.
        """
        cursor = self.sqlite_con.cursor()
        cursor.execute(f'DROP TRIGGER IF EXISTS `{trigger_name}`;')
        self.sqlite_con.commit()

    def read_table(self, table_or_view_name: str) -> pd.DataFrame:
        """
        Select all rows of a table or view and return as a dataframe.
        """
        return pd.read_sql(f'SELECT * FROM `{table_or_view_name}`;', self.sqlite_con)

    def disconnect(self) -> None:
        """
        Close connection to SQLite chat.db.
        """
        self.sqlite_con.close()
        self.logger.info('Closed connection to chat.db')


class ChatDb(SQLiteDb):
    def __init__(self, native_chatdb_path: str, imessage_extractor_db_path: str, logger: logging.Logger) -> None:
        self.logger = logger
        self.sqlite_failed_connection_string = sqlite_failed_connection_string

        # Find native chat.db
        self.native_chatdb_path = expanduser(native_chatdb_path)
        if not isfile(self.native_chatdb_path):
            raise FileNotFoundError(f'chat.db not found at {path(self.native_chatdb_path)}')

        self.logger.info(f'Native chat.db (source): {path(self.native_chatdb_path)}', arrow='black')
        self.logger.info(f'Output chat.db (target): {path(imessage_extractor_db_path)}', arrow='black')

        self.chatdb_cfg = self._load_config()

        # Copy chat.db to a separate location to prevent damage
        self.logger.info('Copy Source Data to Target', bold=True)
        self.logger.info('Querying source chat.db...', arrow='black')
        self.copy(native_chatdb_path=self.native_chatdb_path, imessage_extractor_chatdb_path=imessage_extractor_db_path)
        if isfile(imessage_extractor_db_path):
            self.chatdb_path = imessage_extractor_db_path
        else:
            raise FileNotFoundError(f'Intended copied chat.db not found at {path(imessage_extractor_db_path)}')

        super().__init__(db_path=imessage_extractor_db_path, logger=logger)

        self.sqlite_con = self.connect()
        self.logger.info('Established connection to target', arrow='black')
        del imessage_extractor_db_path

        self._validate_config_chatdb_alignment()

    def _load_config(self):
        """
        Load the chat.db table configuration.
        """
        with open(join(dirname(__file__), 'chatdb_table_info.json'), 'r') as f:
            return json.loads(f.read())

    def _validate_config_chatdb_alignment(self):
        """"
        Validate that the list of tables in the config JSON file matches perfectly with the
        list of tables in chat.db itself. That is, there should be no extra tables in the
        JSON config, and no tables in chat.db that are unaccounted for in the JSON.
        """
        cfg_tables = list(self.chatdb_cfg.keys())
        chatdb_tables = self.list_tables()

        extra_cfg_tables = [x for x in cfg_tables if x not in chatdb_tables]
        extra_chatdb_tables = [x for x in chatdb_tables if x not in cfg_tables]

        if len(extra_cfg_tables):
            raise Exception(f'The following tables exist in {path("chatdb_table_info.json")} but not in chat.db: {extra_cfg_tables}')

        if len(extra_chatdb_tables):
            raise Exception(f'The following tables exist in {path("chatdb_table_info.json")} but not in chat.db: {extra_chatdb_tables}')

        self.logger.info(f'Validated data schema of target against {path("chatdb_table_info.json")}', arrow='black')

    def copy(self, native_chatdb_path: str, imessage_extractor_chatdb_path: str) -> None:
        """
        Copy the entire SQLite database to a separate directory.
        """
        if isfile(imessage_extractor_chatdb_path):
            remove(imessage_extractor_chatdb_path)
        else:
            if not isdir(dirname(imessage_extractor_chatdb_path)):
                mkdir(dirname(imessage_extractor_chatdb_path))

        try:
            native_chatdb_con = sqlite3.connect(native_chatdb_path)
        except Exception as e:
            raise Exception(self.sqlite_failed_connection_string)

        cursor = native_chatdb_con.cursor()
        native_chatdb_tables = [x[0] for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

        copied_chatdb_con = sqlite3.connect(imessage_extractor_chatdb_path)
        copy_command_template = 'sqlite3 {native_chatdb_path} ".dump {table_name}" | sqlite3 {imessage_extractor_chatdb_path}'

        for table_name in native_chatdb_tables:
            copy_sql = copy_command_template.format(**locals())
            subprocess.call(copy_sql, shell=True)


        native_chatdb_con.close()
        copied_chatdb_con.close()

        self.logger.info('Copied chat.db tables to target', arrow='black')
