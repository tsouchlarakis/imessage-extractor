import click
import json
import logging
import shutil
import time
from .chatdb.chatdb import ChatDb, ChatDbTable, View, parse_pg_credentials
from .custom_tables.custom_tables import build_custom_tables
from .helpers.config import WorkflowConfig
from .helpers.verbosity import print_startup_message, logger_setup, path, bold, code
from .quality_control.quality_control import create_qc_views, run_quality_control
from .staging.staging import assemble_staging_order, build_staging_tables_and_views
from .helpers.utils import fmt_seconds, human_filesize
from os import makedirs, stat
from os.path import expanduser, isdir, join
from sql_query_tools import Postgres



@click.option('--chat-db-path', type=str, default=expanduser('~/Library/Messages/chat.db'),
              help='Path to working chat.db.')
@click.option('--save-csv', type=str, default=None, required=False,
              help='Path to folder to save chat.db tables to.')
@click.option('--pg-schema', type=str, default=None, required=False,
              help='Name of Postgres schema to save tables to.')
@click.option('--pg-credentials', type=str, default=None, required=False,
              help="""EITHER the path to a local Postgres credentials
              file 'i.e. ~/.pgpass', OR a string with the connection credentials. Must
              be in format 'hostname:port:db_name:user:pg_pass'.""")
@click.option('-r', '--rebuild', is_flag=True, default=False,
              help='Wipe target Postgres schema and rebuild from scratch.')
@click.option('-s', '--stage', is_flag=True, default=True,
              help='Build staging tables and views after the chat.db tables have been loaded')
@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
@click.option('-d', '--debug', is_flag=True, default=False,
              help='Set logging level to DEBUG.')

@click.command()
def go(chat_db_path,
       save_csv,
       pg_schema,
       pg_credentials,
       rebuild,
       stage,
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

    logger.info(f'Saving chat.db tables to {path(save_csv_dpath)}')

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
            pg.drop_schema_and_recreate(pg_schema, if_exists=True, cascade=True)

            logger.info(f"""Parameter {code("rebuild")} is set to {code("True")},
                so re-created schema "{bold(cfg.pg_schema)}" from scratch""")
        else:
            # Drop views in the Postgres schema since they may be dependent on tables
            # that require rebuilding. They will all be re-created later
            logger.info(f'''Parameter {code("rebuild")} is set to {code("False")},
                so only appending new information from chat.db to "{bold(cfg.pg_schema)}"''')

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

        # Save all tables to Postgres in such an order that foreign keys are resolved correctly.
        # For example, if a table depends on another table, then the other table must be created
        # before the dependent table.

        inserted_journal = []  # Keep a log of all tables that have been saved to Postgres

        while len(inserted_journal) < len(chatdb_tables):
            for table_name, table_object in chatdb_tables.items():
                if cfg.rebuild or table_object.write_mode == 'replace':
                    participle = 'Rebuilt'
                else:
                    participle = 'Refreshed'

                if table_name not in inserted_journal:
                    if table_object.reference is not None:
                        if len([t for t in table_object.reference if t not in inserted_journal]):
                            # There is still one or more reference table that has not yet
                            # been saved to Postgres. Continue to the next table.
                            continue
                        else:
                            # All necessary reference tables for `table_name` have already
                            # been saved to Postgres, so we can now insert this table
                            table_object.save_to_postgres(pg=pg, pg_schema=pg_schema)
                            inserted_journal.append(table_name)
                            logger.info(f'{participle} table "{bold(table_name)}"', arrow='cyan')
                    else:
                        # No references found for this table, we can insert it right away
                        # since there are no dependencies to worry about
                        table_object.save_to_postgres(pg=pg, pg_schema=pg_schema)
                        inserted_journal.append(table_name)
                        logger.info(f'{participle} table "{bold(table_name)}"', arrow='cyan')
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

        logger.info(f'Defining views that are only dependent on chat.db tables')

        with open(cfg.file.chatdb_view_info, 'r') as f:
            chatdb_vw_info = json.load(f)

            for vw_name in chatdb_vw_info:
                if not pg.view_exists(cfg.pg_schema, vw_name):
                    view = View(vw_name=vw_name, vw_type='chatdb', logger=logger, cfg=cfg)
                    view.create(pg=pg, cascade=True)

        if cfg.stage:
            #
            # Staging tables and views
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

            logger.info(f'Staging Postgres tables and views')

            staging_order = assemble_staging_order(pg=pg, cfg=cfg, logger=logger)
            logger.info(f'Staging order: {" > ".join(list(staging_order.keys()))}')

            build_staging_tables_and_views(staging_order=staging_order,
                                           pg=pg,
                                           logger=logger,
                                           cfg=cfg)

        #
        # Quality control views
        #

        logger.info('Creating quality control views')
        create_qc_views(pg=pg, cfg=cfg, logger=logger)

        run_quality_control(pg=pg, cfg=cfg, logger=logger)

    else:
        logger.info('User opted not to save tables to a Postgres database')


    if cfg.save_csv is None:
        logger.info('Cleanup')
        shutil.rmtree(save_csv_dpath)
        logger.info(f'Removed temporary directory {path(save_csv_dpath)}', arrow='red')

    diff_formatted = fmt_seconds(time.time() - start_ts, units='auto', round_digits=2)
    elapsed_time = f"{diff_formatted['value']} {diff_formatted['units']}"
    logger.info(f'{click.style("iMessage Extractor", bold=True)} workflow completed in {elapsed_time}')
