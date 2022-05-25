import click
import logging
import time
from imessage_extractor.src.chatdb.chatdb import ChatDb
from imessage_extractor.src.helpers.config import WorkflowConfig
from imessage_extractor.src.helpers.utils import fmt_seconds
from imessage_extractor.src.helpers.verbosity import bold
from imessage_extractor.src.helpers.verbosity import print_startup_message, logger_setup
from imessage_extractor.src.quality_control.quality_control import create_qc_views, run_quality_control
from imessage_extractor.src.refresh_contacts.refresh_contacts import refresh_contacts
from imessage_extractor.src.staging.staging import assemble_staging_order
from imessage_extractor.src.static_tables.static_tables import build_static_tables
from os.path import expanduser


@click.option('--chatdb-path', type=str, default=expanduser('~/Library/Messages/chat.db'), required=True,
              help='Path to working chat.db, should be in ~/Library/Messages.')
@click.option('--output-db-path', type=str, required=True, default=expanduser('~/Desktop/imessage_extractor_chat.db'),
              help='Desired path to output .db SQLite database file.')
@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
@click.option('-d', '--debug', is_flag=True, default=False,
              help='Set logging level to DEBUG.')

@click.command()
def go(chatdb_path, output_db_path, verbose, debug) -> None:
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

    if verbose:
        print_startup_message(logger)

    # Load workflow configurations
    logger.info('Configure Workflow', bold=True)
    cfg = WorkflowConfig(params=params, logger=logger)

    #
    # Refresh contacts
    #

    logger.info('Refresh Contacts', bold=True)
    refresh_contacts(logger=logger)

    #
    # Establish database connections and copy data from source to target
    #

    logger.info('Establish Database Connections', bold=True)

    output_db_path = expanduser(output_db_path)
    chatdb = ChatDb(native_chatdb_path=chatdb_path, imessage_extractor_db_path=output_db_path, logger=logger)

    logger.info('All subsequent actions apply to the target chat.db', arrow='black')

    #
    # Static tables
    #

    build_static_tables(sqlite_con=chatdb.sqlite_con, logger=logger, cfg=cfg)

    #
    # Staging tables and views
    #

    # At this point in the workflow, all data from chat.db has been loaded into SQLite
    # and static tables (which are overwritten with each run of the workflow) have been
    # build. In addition all views that are reliant ONLY on those chat.db tables have
    # been defined.
    #
    # We'll now begin the process of defining staging tables and views, the dependencies
    # for which are much more fluid, in that a staging view might be dependent on another
    # staging table, which in turn might have a dependency on a different staging
    # view. Because dependencies among staging tables and views may be of arbitrary depth,
    # they require a specific order in which they may be defined.

    logger.info(f'Staging Tables and Views', bold=True)

    assemble_staging_order(chatdb=chatdb, cfg=cfg)
    # logger.debug(f'Staging order: {" > ".join(list(staging_order.keys()))}')

    # build_staging_tables_and_views(staging_order=staging_order,
    #                                chatdb=chatdb,
    #                                logger=logger,
    #                                cfg=cfg)

    #
    # Quality control views
    #

    logger.info('Quality Control', bold=True)
    create_qc_views(chatdb=chatdb, cfg=cfg, logger=logger)
    total_warnings = run_quality_control(chatdb=chatdb, cfg=cfg, logger=logger)

    #
    # End
    #

    diff_formatted = fmt_seconds(time.time() - start_ts, units='auto', round_digits=2)
    elapsed_time = f"{diff_formatted['value']} {diff_formatted['units']}"
    plural_s = '' if total_warnings == 1 else 's'
    warnings_str = click.style(f' with {total_warnings} warning{plural_s}', fg='yellow') if total_warnings > 0 else ''
    logger.info(f'{click.style("iMessage Extractor", bold=True)} workflow completed{warnings_str} in {bold(elapsed_time)}')
