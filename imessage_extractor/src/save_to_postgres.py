import logging
from pydoni import Postgres
from .objects import ChatDbTable, ChatDbExtract
from .verbosity import bold, path
from os.path import abspath, join, dirname, isfile, splitext
from os import listdir


vw_dpath = abspath(join(dirname(__file__), 'views'))


class View(object):
    """
    Store information and operations on a target Postgres iMessage database view.
    """
    def __init__(self, pg_schema: str, vw_name: str, pg: Postgres, logger: logging.Logger) -> None:
        self.pg_schema = pg_schema
        self.vw_name = splitext(vw_name)[0]  # Handles the case that vw_name contains a file extension
        self.pg = pg
        self.logger = logger

        self.fpath = join(vw_dpath, self.vw_name + '.sql')
        if not isfile(self.fpath):
            raise FileNotFoundError(f'View {bold(self.vw_name)} definition expected at {path(self.fpath)} but not found')

        with open(self.fpath, 'r') as f:
            self.def_sql = f.read()

        self.logger.info(f'View {bold(self.vw_name)}', arrow='white')

    def drop(self) -> None:
        """
        Drop the target view if it exists.
        """
        if self.pg.view_exists(self.pg_schema, self.vw_name):
            self.pg.drop_view_if_exists(self.pg_schema, self.vw_name)
            self.logger.info(f'Removed view {bold(self.vw_name)}')

    def create(self) -> None:
        """
        Execute view definition SQL.
        """
        self.pg.execute(self.def_sql)
        self.logger.info(f'Created view {bold(self.vw_name)}')


class Table(object):
    """
    Store information and operations on a target Postgres iMessage database table.
    """
    def __init__(self, chat_db_table: ChatDbTable, pg_schema: str, pg: Postgres, logger: logging.Logger) -> None:
        self.__dict__.update(chat_db_table.__dict__)
        self.pg_schema = pg_schema
        self.pg = pg
        self.logger = logger
        self.logger.info(f'Table {bold(self.table_name)}', arrow='white')

    def create(self, rebuild: bool):
        """
        Create a target table.
        """
        if rebuild:
            # Simply overwrite target table if exists, else create it from scratch
            write_mode = 'replace'
        else:
            write_mode = self.write_mode  # Can only be 'replace' or 'append'

        assert write_mode in ['replace', 'append'], f'Invalid write mode "{write_mode}"'

        self.df.to_sql(name=self.table_name,
                        con=self.pg.dbcon,
                        schema=self.pg_schema,
                        index=False,
                        if_exists=write_mode)

        if write_mode == 'replace':
            self.logger.info(f'Rebuilt table {bold(self.table_name)}', arrow='white')
        elif write_mode == 'append':
            self.logger.info(f'Appended data to table {bold(self.table_name)}', arrow='white')



def save_to_postgres(pg: Postgres,
                     chat_db_extract: ChatDbExtract,
                     pg_schema: str,
                     rebuild: bool,
                     logger: logging.Logger) -> None:
    """
    Accept a database extract object and load each table into Postgres.
    """
    logger.info(f'Loading data into schema {bold(pg_schema)}')

    if rebuild:
        # Drop all objects in the Postgres schema to rebuild it from scratch
        logger.info('Fully rebuilding destination database objects')
        pg.execute(f'drop schema if exists {pg_schema} cascade')
        pg.execute(f'create schema {pg_schema}')
        # pg.drop_schema_if_exists_and_recreate(pg_schema, cascade=True)  # TODO: uncomment when new version of pydoni released
        logger.info(f'Re-created schema from scratch')

    # Load all table and view objects into python objects
    logger.info('Initializing target table and view python objects')
    vw_names = [f for f in listdir(vw_dpath) if splitext(f)[1] == '.sql']
    vw_objects = [View(pg_schema, vw_name, pg, logger) for vw_name in vw_names]
    del vw_names

    table_objects = [Table(table_object, pg_schema, pg, logger) for table_name, table_object in chat_db_extract.table_objects.items()]

    if not rebuild:
        # Drop views before refreshing tables as they may be dependent on each other
        for vw in vw_objects:
            vw.drop()

    # At this point, target views definitely do not exist, whereas target tables may or may
    # not exist, depending on whether `rebuild` is True or not. If True, then we can assume
    # tables were wiped when the schema was dropped above. If False, the tables will not
    # exist if this is the first time this pipeline is being run, but will likely
    # otherwise exist.

    # Write chat.db tables to Postgres
    logger.info('Loading table data into Postgres')
    for table_object in table_objects:
        table_object.create(rebuild)
