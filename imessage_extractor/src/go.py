import click
import json
import logging
import pathlib
import shutil
import time
from pydoni import advanced_strip, fmt_seconds, human_filesize, Postgres
from .chatdb.chatdb import ChatDb, ChatDbTable, View, parse_pg_credentials
from .custom_tables.custom_tables import CustomTable, build_custom_tables
from .helpers.config import WorkflowConfig
from .helpers.verbosity import print_startup_message, logger_setup, path, bold, code
from .staging.staging import build_staging_tables, assemble_staging_order
from os import makedirs, stat
from os.path import expanduser, isdir, join


# vw_dpath = abspath(join(dirname(__file__), 'views'))
# vw_def_dpath = join(vw_dpath, 'definitions')
# custom_table_dpath = abspath(join(dirname(__file__), '..', 'custom_tables'))
logger = logger_setup(name='imessage-extractor', level=logging.ERROR)


# def validate_vw_info(vw_names: str) -> None:
#     """
#     Validate that the .sql view files in the `vw_def_dpath` folder are compatible with
#     corresponding view metadata in the vw_info_*.json files.
#     """
#     # Views that can be defined after chat.db tables are loaded
#     with open(join(vw_dpath, 'view_info_chatdb_dependent.json'), 'r') as f:
#         vw_info_chat_db_dependent = json.load(f)

#     # Views that depend on tables created downstream in this pipeline
#     with open(join(vw_dpath, 'view_info_staging_dependent.json'), 'r') as f:
#         vw_info_staging_dependent = json.load(f)

#     # Validate that all views defined in vw_info*.json also contain a definition .sql
#     # file in the views/ folder, and that all views with definition .sql files in the
#     # views/ folder also have a corresponding key in the vw_info*.json file.

#     for vw_name in vw_names:
#         view_names_all = list(vw_info_chat_db_dependent.keys()) + list(vw_info_staging_dependent.keys())
#         if vw_name not in view_names_all:
#             raise ValueError(advanced_strip(
#                 f"""View definition {bold(vw_name)} found at
#                 {path(join(vw_def_dpath, vw_name + ".sql"))}
#                 but not accounted for in {path("view_info.json")}"""))

#     for vw_name in view_names_all:
#         if vw_name not in vw_names:
#             raise ValueError(advanced_strip(
#                 f"""View definition {bold(vw_name)} found in {path("view_info.json")}
#                 but not accounted for at {path(join(vw_def_dpath, vw_name + ".sql"))}"""))


@click.option('--chat-db-path', type=str, default=expanduser('~/Library/Messages/chat.db'),
              help='Path to working chat.db.')
@click.option('--save-csv', type=str, default=None, required=False,
              help='Path to folder to save chat.db tables to.')
@click.option('--pg-schema', type=str, default=None, required=False,
              help='Name of Postgres schema to save tables to.')
@click.option('--pg-credentials', type=str, default=expanduser('~/.pgpass'), required=False,
              help=advanced_strip("""EITHER the path to a local Postgres credentials
              file 'i.e. ~/.pgpass', OR a string with the connection credentials. Must
              be in format 'hostname:port:db_name:user:pg_pass'."""))
@click.option('-r', '--rebuild', is_flag=True, default=False,
              help='Wipe target Postgres schema and rebuild from scratch.')
@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
@click.option('--debug', is_flag=True, default=False,
              help='Set logging level to DEBUG.')

@click.command()
def go(chat_db_path,
       save_csv,
       pg_schema,
       pg_credentials,
       rebuild,
       verbose,
       debug) -> None:
    """
    Run the imessage-extractor!
    """
    params = locals()

    #
    # Workflow configuration
    #

    # Configure logger
    if debug:
        logging_level = logging.DEBUG
    elif verbose:
        logging_level = logging.INFO
    else:
        logging_level = logging.ERROR

    logger = logger_setup(name='imessage-extractor', level=logging_level)

    # Begin pipeline stopwatch
    start_ts = time.time()

    # Get workflow configurations
    cfg = WorkflowConfig(params=params, logger=logger)

    if verbose:
        print_startup_message(logger)

    #
    # Establish database connections
    #

    chatdb = ChatDb(chat_db_path=chat_db_path, logger=logger)

    if cfg.pg_schema is not None:
        hostname, port, db_name, pg_user, pw = parse_pg_credentials(cfg.pg_credentials)
        pg = Postgres(hostname=hostname,
                             port=port,
                             db_name=db_name,
                             pg_user=pg_user,
                             pw=pw)

        logger.info(f'Connected to Postgres database {bold(db_name)} hosted on {bold(hostname + ":" + port)}')

    # Create a temporary folder to save the extracted data to if the user opted not
    # to save the outputted .csv files to a local folder
    save_csv_dpath = expanduser('~/Desktop/.tmp_imessage_extractor') if cfg.save_csv is None else cfg.save_csv
    if not isdir(save_csv_dpath):
        makedirs(save_csv_dpath)
        logger.info(f'Created temporary export directory {path(save_csv_dpath)}')

    #
    # Extract metadata for each table in chat.db. If `rebuild` is True, then we get
    # all data in chat.db. If False and we're inserting into Postgres, then we query
    # each chat.db table in Postgres if it exists and only query the chat.db table
    # above the maximum numerical index in the Postgres table. This drastically reduces
    # the computational time required, because we only need to upload *new* data to
    # Postgres, instead of refreshing the full tables on each run.
    #
    # Then save tables to .csv files.
    #

    with open(cfg.file.chatdb_table_info, 'r') as json_file:
        chatdb_table_info = json.load(json_file)

    chatdb_tables = {}

    for table_name in list(chatdb_table_info.keys()):
        table_object = ChatDbTable(table_name=table_name, logger=logger, cfg=cfg)
        table_object.get_shape(sqlite_con=chatdb.sqlite_con)

        if cfg.pg_schema is not None:
            table_object.build_pg_create_table_sql(sqlite_con=chatdb.sqlite_con)
            max_pg_pkey_value_dct = table_object.query_primary_key_max_pg_values(pg=pg)
        else:
            max_pg_pkey_value_dct = None

        output_csv_fpath = join(save_csv_dpath, table_name + '.csv')
        table_object.save_to_csv(output_fpath=output_csv_fpath,
                                 sqlite_con=chatdb.sqlite_con,
                                 max_pg_pkey_values=max_pg_pkey_value_dct)

        file_size_str = human_filesize(stat(output_csv_fpath).st_size)
        chatdb_tables[table_name] = table_object
        logger.info(
            f"""Saved SQLite:{bold(table_name)} to {path(table_name + '.csv')}
            ({file_size_str}), shape {table_object.shape}
            """, arrow='white')

    #
    # Refresh target Postgres schema
    #

    if cfg.pg_schema is not None:
        # Drop all objects in the Postgres schema in order to rebuild it from scratch
        if cfg.rebuild:
            pg.drop_schema(cfg.pg_schema, if_exists=True, cascade=True)
            pg.create_schema(cfg.pg_schema)
            # TODO: uncomment on new pydoni release
            # pg.drop_schema_and_recreate(pg_schema, if_exists=True, cascade=True)

            logger.info(advanced_strip(
                f"""Parameter {code("rebuild")} is set to {code("True")},
                so re-created schema "{bold(cfg.pg_schema)}" from scratch"""))
        else:
            # Drop views in the Postgres schema since they may be dependent on tables
            # that require rebuilding. They will all be re-created later
            logger.info(advanced_strip(
                f'''Parameter {code("rebuild")} is set to {code("False")},
                so only appending new information from chat.db to "{bold(cfg.pg_schema)}"'''))

            with open(cfg.file.chatdb_view_info, 'r') as json_file:
                chatdb_view_info = json.load(json_file)

            for vw_name in chatdb_view_info:
                if pg.view_exists(cfg.pg_schema, vw_name):
                    logger.debug(f'Dropping chatdb view {bold(vw_name)}')
                    view = View(vw_name=vw_name, vw_type='chatdb', logger=logger, cfg=cfg)
                    view.drop(pg=pg, if_exists=True, cascade=True)

            with open(cfg.file.staging_view_info, 'r') as json_file:
                staging_view_info = json.load(json_file)

            for vw_name in staging_view_info:
                if pg.view_exists(cfg.pg_schema, vw_name):
                    logger.debug(f'Dropping staging view {bold(vw_name)}')
                    view = View(vw_name=vw_name, vw_type='staging', logger=logger, cfg=cfg)
                    view.drop(pg=pg, if_exists=True, cascade=True)

        #
        # chat.db tables
        #

        logger.info(f'Saving tables to schema "{bold(cfg.pg_schema)}"')

        """
        Save all tables to Postgres in such an order that foreign keys are resolved correctly.
        For example, if a table depends on another table, then the other table must be created
        before the dependent table.
        """
        inserted_journal = []  # Keep a log of all tables that have been saved to Postgres

        while len(inserted_journal) < len(chatdb_tables):
            for table_name, table_object in chatdb_tables.items():
                if cfg.rebuild or table_object.write_mode == 'replace':
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

        #
        # Custom tables
        #

        build_custom_tables(pg=pg, logger=logger, cfg=cfg)

        #
        # Chat.db dependent views
        #

        logger.info(f'Defining Postgres views that are only dependent on chat.db tables')

        with open(cfg.file.chatdb_view_info, 'r') as f:
            chatdb_vw_info = json.load(f)

            for vw_name in chatdb_vw_info:
                if not pg.view_exists(cfg.pg_schema, vw_name):
                    view = View(vw_name=vw_name, vw_type='chatdb', logger=logger, cfg=cfg)
                    view.create(pg=pg, cascade=True)

        #
        # Staging tables
        #
        # At this point in the workflow, all data from chat.db has been loaded into Postgres
        # and custom tables (which are overwritten with each run of the workflow) have been
        # build. In addition all views that are reliant ONLY on those chat.db tables have
        # been defined.
        #
        # We'll now begin the process of defining staging tables and views, the dependencies
        # for which are much more fluid, in that a staging view might be dependent on another
        # staging table, which in turn might have a dependency on a different staging
        # view. Because dependencies among staging tables and views may be of arbitrary depth,
        # they require a specific order in which they may be defined.
        #
        #

        # build_staging_tables(pg=pg, logger=logger, cfg=cfg)

        staging_order = assemble_staging_order(pg=pg, cfg=cfg, logger=logger)

        if cfg.rebuild:
            # Create each object in the `staging_order` in order
            pass

        else:
            # If we're not refreshing the pipeline, then the staging tables should
            # already exist. Verify to make sure that is the case, then we can
            # refresh the staging tables then re-create the views.
            pass


    else:
        logger.info('User opted not to save tables to a Postgres database')

    logger.info('Cleanup')

    if cfg.save_csv is None:
        shutil.rmtree(save_csv_dpath)
        logger.info(f'Removed temporary directory {path(save_csv_dpath)}', arrow='red')

    diff_formatted = fmt_seconds(time.time() - start_ts, units='auto', round_digits=2)
    elapsed_time = f"{diff_formatted['value']} {diff_formatted['units']}"
    logger.info(f'iMessage Extractor workflow completed in {elapsed_time}')
