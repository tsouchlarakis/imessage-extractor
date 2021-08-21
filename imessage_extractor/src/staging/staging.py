import json
import logging
import typing
from collections import OrderedDict
from numpy import e
from ..chatdb.chatdb import View
from ..helpers.config import WorkflowConfig
from ..helpers.verbosity import bold, path
from .tables.emoji_text_map import refresh_emoji_text_map
from .tables.message_tokens import refresh_message_tokens
from .tables.tokens import refresh_tokens
from os.path import basename
from pydoni import Postgres, advanced_strip, ensurelist


class StagingTable(object):
    """
    Store information and operations for tables that get staged after chat.db
    data has been loaded into Postgres.
    """
    def __init__(self,
                 table_name: str,
                 refresh_function: typing.Callable,
                 pg: Postgres,
                 logger: logging.Logger,
                 cfg: 'WorkflowConfig') -> None:
        self.pg = pg
        self.table_name = table_name
        self.refresh_function = refresh_function
        self.logger = logger
        self.cfg = cfg

        self.logger.debug(f'Initializing staging table {self.table_name}')

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
        self.reference = json_data['reference']
        self.check_references(self.reference)

        assert isinstance(self.columnspec, dict), \
            f'Columnspec for {self.table_name} must be a dictionary'
        assert isinstance(self.primary_key, str) or isinstance(self.primary_key, list), \
            f'Primary key for {self.table_name} must be a string or list'
        assert self.reference is None or isinstance(self.reference, list), \
            f'References for {self.table_name} must be None or a list'

    def check_references(self, reference) -> None:
        """
        Return True if all reference objects exist in Postgres schema, and return
        an error otherwise.
        """
        if isinstance(reference, list):
            missing_refs = []
            for ref in reference:
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


def assemble_staging_order(pg: Postgres, cfg: WorkflowConfig, logger: logging.Logger) -> OrderedDict:
    """
    Return a dictionary of staging tables and/or views in the order that they should be created.
    """
    with open(cfg.file.staging_table_info) as f:
        staging_table_info = json.load(f)

    with open(cfg.file.staging_view_info) as f:
        staging_vw_info = json.load(f)

    object_info = {}

    for table_name, table_info in staging_table_info.items():
        exists = pg.table_exists(cfg.pg_schema, table_name)
        object_info[table_name] = dict(
            type='table',
            reference=table_info['reference'],
            exists=exists)

    for vw_name, vw_info in staging_vw_info.items():
        exists = pg.table_exists(cfg.pg_schema, vw_name)
        object_info[vw_name] = dict(
            type='view',
            reference=vw_info['reference'],
            exists=exists)

    object_info_nonexistent_refs = {}
    for obj_name, obj_info in object_info.items():
        if not obj_info['exists'] or obj_info['type'] == 'table':
            object_info_nonexistent_refs[obj_name] = dict(type=obj_info['type'], reference={})
            if obj_info['reference'] is not None:
                reference = ensurelist(obj_info['reference'])
                for ref in reference:
                    if ref in staging_table_info:
                        ref_type = 'table'
                        ref_exists = pg.table_exists(cfg.pg_schema, ref)
                    elif ref in staging_vw_info:
                        ref_type = 'view'
                        ref_exists = pg.view_exists(cfg.pg_schema, ref)
                    else:
                        ref_exists = pg.table_or_view_exists(cfg.pg_schema, ref)

                    if not ref_exists:
                        if ref not in staging_table_info and ref not in staging_vw_info:
                            raise ValueError(advanced_strip(
                                f"""Nonexistent reference {bold(ref)} for
                                staging {obj_info['type']} {bold(obj_name)} must itself be a staging
                                table or view, so it must be present in either
                                {path(cfg.file.staging_table_info)} or
                                {path(cfg.file.staging_view_info)}"""))

                        object_info_nonexistent_refs[obj_name]['reference'][ref] = ref_type

    # At this point, we have a dictionary, `object_info_nonexistent_refs` that contains
    # information on staging tables and/or views that currently do not exist, and the
    # reference table or views that will be required to exist when create those tables and/or
    # views. Now let's iterate along that dictionary to define an order in which these objects
    # should be created.

    {
        'contact_token_usage_vw': {
            'reference': {},
            'type': 'view'
        },
        'contact_top_token_usage_by_length': {
            'reference': {'contact_token_usage_vw': 'view'},
            'type': 'view'
        },
        'message_emoji_map': {
            'reference': {},
            'type': 'view'
        },
        'stats_by_contact': {
            'reference': {'contact_token_usage_vw': 'view'},
            'type': 'table'
        }
    }

    staging_order = OrderedDict()

    while len(staging_order) < len(object_info_nonexistent_refs):
        for obj_name, obj_info in object_info_nonexistent_refs.items():
            if obj_name in staging_order:
                # Order already assigned
                pass

            else:
                # No order assigned
                if len(obj_info['reference']) == 0:
                    # No references, so this object can be created
                    staging_order[obj_name] = obj_info['type']
                elif all(r in staging_order for r in obj_info['reference']):
                    # All references has already been assigned, so this object can
                    # be assigned its order
                    staging_order[obj_name] = obj_info['type']
                else:
                    # At least one nonexistent reference that must come before this object.
                    # Continue loop without assigning this object's order yet.
                    pass

    return staging_order


def build_staging_tables_and_views(staging_order: OrderedDict,
                                   pg: Postgres,
                                   logger: logging.Logger,
                                   cfg: WorkflowConfig) -> None:
    """
    Build each object specified in `staging_order` sequentially. Objects may be
    tables or views.
    """
    if len(staging_order):
        table_refresh_functions = dict(
            emoji_text_map=refresh_emoji_text_map,
            message_tokens=refresh_message_tokens,
            tokens=refresh_tokens,
        )

        for item_name, item_type in staging_order.items():
            if item_type == 'table':
                if item_name not in table_refresh_functions:
                    raise ValueError(advanced_strip(
                        f"""Attempting to refresh table {bold(item_name)} (found in
                        staging_table_info.json) but corresponding refresh function
                        not found in `table_refresh_functions` dictionary. Please add
                        the function to the `table_refresh_functions` dictionary, or remove
                        it from staging_table_info.json.
                        """))

                table_object = StagingTable(
                    table_name=item_name,
                    refresh_function=table_refresh_functions[item_name],
                    pg=pg,
                    logger=logger,
                    cfg=cfg
                )

                table_object.refresh()

            elif item_type == 'view':
                logger.info(f'Defining view "{bold(cfg.pg_schema)}"."{bold(item_name)}"', arrow='green')

                view_object = View(
                    vw_name=item_name,
                    vw_type='staging',
                    logger=logger,
                    cfg=cfg
                )

                view_object.create(pg=pg, cascade=False)
    else:
        logger.warning('No staging tables or view definitions found')
