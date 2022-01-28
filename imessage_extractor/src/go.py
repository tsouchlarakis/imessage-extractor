import click
import logging
import time
from .chatdb.chatdb import ChatDb
from .helpers.config import WorkflowConfig
from .helpers.utils import fmt_seconds
from .helpers.verbosity import print_startup_message, logger_setup
from .quality_control.quality_control import create_qc_views, run_quality_control
from .staging.staging import assemble_staging_order
from .static_tables.static_tables import build_static_tables
from imessage_extractor.src.helpers.verbosity import bold
from os.path import expanduser


@click.option('--chatdb-path', type=str, default=expanduser('~/Library/Messages/chat.db'),
              help='Path to working chat.db, should be in ~/Library/Messages.')
@click.option('--outputdb-path', type=str,
              help='Desired path to output .db SQLite database file.')
@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
@click.option('-d', '--debug', is_flag=True, default=False,
              help='Set logging level to DEBUG.')

@click.command()
def go(chatdb_path, outputdb_path, verbose, debug) -> None:
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

    logger.info('Configure Workflow', bold=True)

    # Load workflow configurations
    cfg = WorkflowConfig(params=params, logger=logger)

    #
    # Establish database connections and copy data from source to target
    #

    logger.info('Establish Database Connections', bold=True)

    outputdb_path = expanduser(outputdb_path)
    chatdb = ChatDb(native_chatdb_path=chatdb_path, imessage_extractor_db_path=outputdb_path, logger=logger)

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

    run_quality_control(chatdb=chatdb, cfg=cfg, logger=logger)

    #
    # End
    #

    diff_formatted = fmt_seconds(time.time() - start_ts, units='auto', round_digits=2)
    elapsed_time = f"{diff_formatted['value']} {diff_formatted['units']}"
    logger.info(f'{click.style("iMessage Extractor", bold=True)} workflow completed in {bold(elapsed_time)}')
