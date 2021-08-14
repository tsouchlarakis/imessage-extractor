import json
import logging
import typing
from ..helpers.config import WorkflowConfig
from ..helpers.verbosity import bold
from .tables.emoji_text_map import refresh_emoji_text_map
from .tables.message_tokens import refresh_message_tokens
from .tables.stats_by_contact import refresh_stats_by_contact
from .tables.tokens import refresh_tokens
from os.path import basename
from pydoni import Postgres, advanced_strip


class StagingTable(object):
    """
    Store information and operations for tables that get staged after chat.db
    data has been loaded into Postgres.
    """
    def __init__(self,
                 pg: Postgres,
                 table_name: str,
                 refresh_function: typing.Callable,
                 logger: logging.Logger,
                 cfg: 'WorkflowConfig') -> None:
        self.pg = pg
        self.table_name = table_name
        self.refresh_function = refresh_function
        self.logger = logger
        self.cfg = cfg

        with open(self.cfg.file.staging_table_info) as f:
            json_data = json.load(f)
            if self.table_name in json_data.keys():
                json_data = json_data[self.table_name]
            else:
                raise KeyError(advanced_strip(
                    f"""Table {table_name} expected as a key in
                    {basename(self.cfg.file.staging_table_info)} but not found"""))

        self.columnspec = json_data['columnspec']
        self.primary_key = json_data['primary_key']
        self.references = json_data['references']
        self.check_references(self.references)

        assert isinstance(self.columnspec, dict), \
            f'Columnspec for {self.table_name} must be a dictionary'
        assert isinstance(self.primary_key, str) or isinstance(self.primary_key, list), \
            f'Primary key for {self.table_name} must be a string or list'
        assert self.references is None or isinstance(self.references, list), \
            f'References for {self.table_name} must be None or a list'

    def check_references(self, references) -> None:
        """
        Return True if all reference objects exist in Postgres schema, and return
        an error otherwise.
        """
        if isinstance(references, list):
            missing_refs = []
            for ref in references:
                if not self.pg.table_or_view_exists(self.cfg.pg_schema, ref):
                    missing_refs.append(ref)

            if len(missing_refs) > 0:
                raise Exception(advanced_strip(
                    f"""Staging table {bold(self.table_name)} requires the
                    following non-existent references: {str(missing_refs)}"""))

    def refresh(self) -> None:
        """
        Execute custom refresh function for a particular table. Refresh functions are
        stored as python modules and live in relative directory refresh_functions/
        """
        self.refresh_function(pg=self.pg,
                              pg_schema=self.cfg.pg_schema,
                              table_name=self.table_name,
                              columnspec=self.columnspec,
                              logger=self.logger)


def build_staging_tables(pg: Postgres, logger: logging.Logger, cfg: WorkflowConfig) -> None:
    """
    Build each staged table sequentially. Import each table's refresh function into
    this script, and add an entry in the `refresh_map` for each table that you'd
    like to refresh.
    """
    logger.info('Building staging tables')

    refresh_map = dict(
        emoji_text_map=refresh_emoji_text_map,
        message_tokens=refresh_message_tokens,
        tokens=refresh_tokens,
        # stats_by_contact=refresh_stats_by_contact,
    )

    for table_name, refresh_function in refresh_map.items():
        table_obj = StagingTable(pg=pg,
                                 table_name=table_name,
                                 refresh_function=refresh_function,
                                 logger=logger,
                                 cfg=cfg)

        table_obj.refresh()
