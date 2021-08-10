import click
import json
import logging
import pandas as pd
import pathlib
import pydoni
import shutil
import sqlite3
import time
import typing
from .objects import ChatDbExtract, View
from .verbosity import print_startup_message, logger_setup, path, bold
from .tables.staging import build_staging_tables
from os import makedirs, listdir
from os.path import expanduser, isfile, isdir, splitext, abspath, dirname, join, basename


vw_dpath = abspath(join(dirname(__file__), 'views'))
vw_def_dpath = join(vw_dpath, 'definitions')
custom_table_dpath = abspath(join(dirname(__file__), '..', 'custom_tables'))
logger = logger_setup(name='imessage-extractor', level=logging.ERROR)


def validate_parameters(params: dict) -> None:
    """
    Carry out assertions on parameters.
    """
    if params['save_csv'] is None:
        if params['pg_schema'] is None:
            raise ValueError('Must specify either --save-csv or --save-pg-schema')

    if (params['pg_schema'] is not None and params['pg_credentials'] is None) \
        or (params['pg_schema'] is None and params['pg_credentials'] is not None):
            raise ValueError('Must specify both --pg-credentials and --save-pg-schema if one is specified')

    # Log parameter values
    params = {k: v for k, v in locals().items() if k not in ['logger', 'logging_level']}
    for name, value in params.items():
        logger.debug(f'{name}: {value}')


def connect_to_chat_db(chat_db_path: typing.Union[str, pathlib.Path]) -> sqlite3.Connection:
    """
    Connect to local chat.db.
    """
    if isfile(chat_db_path):
        try:
            sqlite_con = sqlite3.connect(chat_db_path)
            logger.info(f'Connected to chat.db {path(chat_db_path)}')
            return sqlite_con
        except Exception as e:
            raise(Exception(pydoni.advanced_strip("""Unable to connect to SQLite! Could it be
            that the executing environment does not have proper permissions? Perhaps wrapping
            the command in an (Automator) application or script, and granting Full Disk
            Access to that application or script might be a potential option""")))
    else:
        raise FileNotFoundError(f'The chat.db file expected at {path(chat_db_path)} could not be found')


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
        pydoni.advanced_strip("""Invalid structure of supplied Postgres credentials.
        Must be either a path to a local Postgres credentials file 'i.e. ~/.pgpass', OR
        a string with the connection credentials. Must be in format
        'hostname:port:db_name:user_name:password'.""")

    hostname, port, db_name, pg_user, pw = pg_cred_str.split(':')
    return hostname, port, db_name, pg_user, pw


def list_view_names(vw_def_dpath: typing.Union[str, pathlib.Path]) -> list:
    """
    Find views in folder.
    """
    return [f.replace('.sql', '') for f in listdir(vw_def_dpath) if splitext(f)[1] == '.sql']


def build_custom_tables(logger: logging.Logger, pg: pydoni.Postgres, pg_schema: str) -> None:
    """
    Build custom, user-maintained tables, the information for which is stored in the
    'custom_tables' folder.
    """
    custom_table_fpaths = [abspath(x) for x in listdir(custom_table_dpath) if not x.startswith('.')]
    any_manual_tabe_defs_exist = len(custom_table_fpaths) > 0

    if any_manual_tabe_defs_exist:
        logger.info('Building optional custom tables')
        schemas_fpath = join(custom_table_dpath, 'custom_table_schemas.json')
        if isfile(schemas_fpath):
            custom_table_csv_fpaths = [x for x in custom_table_fpaths if splitext(x)[1] == '.csv']

            # Validate that for each custom table, there exists both a .csv file with table
            # data, and a key in custom_table_schemas.json with the same name as the .csv file

            with open(schemas_fpath, 'r') as json_file:
                schemas = json.load(json_file)

            for csv_fpath in custom_table_csv_fpaths:
                table_name = splitext(basename(csv_fpath))[0]
                assert table_name in schemas.keys(), \
                    pydoni.advanced_strip(f"""Attempting to define custom table {bold(table_name)},
                    the table data .csv file exists at {path(csv_fpath)} but a column
                    specification does not exist in {path(schemas_fpath)} for that table.
                    Add a key in that JSON file with the same name as the .csv file
                    with a corresponding value that is a dictionary with a name: dtype pair
                    for each column in the .csv file. For example, if the .csv file has
                    columns ['id', 'message'], then add an entry to that JSON file:
                    "{table_name}": {{"id": "INTEGER", "message": "TEXT"}}""")

            for table_name in schemas.keys():
                expected_csv_fpath = join(custom_table_dpath, table_name + '.csv')
                assert isfile(expected_csv_fpath), \
                    pydoni.advanced_strip(f"""Attempting to define custom table {bold(table_name)},
                    but the table data .csv file does not exist at {path(expected_csv_fpath)}.
                    Please create this .csv file or remove the key in {path(schemas_fpath)}.
                    """)

            # Now that we know there exists a .csv and a JSON key for each custom table,
            # we can proceed with validating that the table schemas defined in the JSON file
            # are compatible with the data contained the corresponding .csv files.

            for table_name, schema_dct in schemas.items():
                csv_fpath = join(custom_table_dpath, table_name + '.csv')
                csv_df = pd.read_csv(csv_fpath)

                for col_name, col_dtype in schema_dct.items():
                    # Validate that column specified in JSON config actually exists
                    # in the .csv file
                    assert col_name in csv_df.columns, \
                        f'Column {bold(col_name)} specified in {path(schemas_fpath)} but does not exist in {path(csv_fpath)}'

                for col_name in csv_df.columns:
                    assert col_name in schema_dct.keys(), \
                        f'Column {bold(col_name)} exists in {path(csv_fpath)} but does not exist in {path(schemas_fpath)}'

                # At this point, we've validated that there is total alignment between
                # this table and its respective columns specified in the JSON config file
                # and as .csv file in the `custom_table_dpath` itself. Now we can proceed
                # to actually load the table into the Postgres schema.

                csv_df.to_sql(name=table_name,
                              con=pg.dbcon,
                              schema=pg_schema,
                              index=False,
                              if_exists='replace')

                logger.info(f'Rebuilt Postgres:{bold(table_name)}', arrow='white')

        else:
            raise FileNotFoundError(f'Could not find custom table schemas file {schemas_fpath}')
    else:
        logger.warning(pydoni.advanced_strip(f"""
        No custom table data exists, so no custom tables were created. This is perfectly
        okay and does not affect the running of this pipeline, however know that if you'd
        like to add a table to this pipeline, you can manually create a .csv file and
        place it in the {path(custom_table_dpath, 'table_data')} folder. The resulting
        table will be dropped and re-created with each run of this pipeline, and will
        be named identically to how you choose to name the .csv file. NOTE: each .csv file
        must be accompanied by a .json file that specifies the table schema, or else
        an error will be thrown. That table schema should be in the format:
        {{column_name1: postgres_data_type1, column_name2: postgres_data_type2, ...}}"""))


def validate_vw_info(vw_names: str) -> None:
    """
    Validate that the .sql view files in the `vw_def_dpath` folder are compatible with
    corresponding view metadata in the vw_info_*.json files.
    """
    # Views that can be defined after chat.db tables are loaded
    with open(join(vw_dpath, 'view_info_chatdb_dependent.json'), 'r') as f:
        vw_info_chat_db_dependent = json.load(f)

    # Views that depend on tables created downstream in this pipeline
    with open(join(vw_dpath, 'view_info_staging_dependent.json'), 'r') as f:
        vw_info_staging_dependent = json.load(f)

    # Validate that all views defined in vw_info*.json also contain a definition .sql
    # file in the views/ folder, and that all views with definition .sql files in the
    # views/ folder also have a corresponding key in the vw_info*.json file.

    for vw_name in vw_names:
        view_names_all = list(vw_info_chat_db_dependent.keys()) + list(vw_info_staging_dependent.keys())
        if vw_name not in view_names_all:
            raise Exception(pydoni.advanced_strip(f"""
            View definition {bold(vw_name)} found at {path(join(vw_def_dpath, vw_name + ".sql"))}
            but not accounted for in {path("view_info.json")}"""))

    for vw_name in view_names_all:
        if vw_name not in vw_names:
            raise Exception(pydoni.advanced_strip(f"""
            View definition {bold(vw_name)} found in {path("view_info.json")} but not
            accounted for at {path(join(vw_def_dpath, vw_name + ".sql"))}"""))


@click.option('--chat-db-path', type=str, default=expanduser('~/Library/Messages/chat.db'),
              help='Path to working chat.db.')
@click.option('--save-csv', type=str, default=None, required=False,
              help='Path to folder to save chat.db tables to.')
@click.option('--pg-schema', type=str, default=None, required=False,
              help='Name of Postgres schema to save tables to.')
@click.option('--pg-credentials', type=str, default=expanduser('~/.pgpass'), required=False,
              help=pydoni.advanced_strip("""EITHER the path to a local Postgres credentials
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

    # Configure logger
    if debug:
        logging_level = logging.DEBUG
    elif verbose:
        logging_level = logging.INFO
    else:
        logging_level = logging.ERROR
    logger = logger_setup(name='imessage-extractor', level=logging_level)

    validate_parameters(params)

    # Begin pipeline stopwatch
    start_ts = time.time()

    if verbose:
        print_startup_message(logger)

    # Connect to the local chat.db
    sqlite_con = connect_to_chat_db(chat_db_path=chat_db_path)

    if pg_schema is not None:
        # Get Postgres credentials and connect to database
        hostname, port, db_name, pg_user, pw = parse_pg_credentials(pg_credentials)
        pg = pydoni.Postgres(hostname=hostname,
                             port=port,
                             db_name=db_name,
                             pg_user=pg_user,
                             pw=pw)

        logger.info(f'Connected to Postgres database {bold(db_name)} hosted on {bold(hostname + ":" + port)}')

    # Create a temporary folder to save the extracted data to if the user opted not
    # to save the outputted .csv files to a local folder
    save_csv_dpath = expanduser('~/Desktop/.tmp_imessage_extractor') if save_csv is None else save_csv
    if not isdir(save_csv_dpath):
        makedirs(save_csv_dpath)
        logger.info(f'Created temporary export directory {path(save_csv_dpath)}')

    # Extract metadata for each table in chat.db
    chat_db_extract = ChatDbExtract(sqlite_con=sqlite_con,
                                    pg=pg,
                                    pg_schema=pg_schema,
                                    rebuild=rebuild,
                                    logger=logger)

    # Save tables to .csv files
    logger.info(f'Saving tables to {path(save_csv_dpath)}')
    chat_db_extract.save_to_csv(dir_name=save_csv_dpath, logger=logger)

    #
    # Refresh target Postgres schema
    #

    if pg_schema is not None:
        # Drop all objects in the Postgres schema in order to rebuild it from scratch
        if rebuild:
            pg.drop_schema(pg_schema, if_exists=True, cascade=True)
            pg.create_schema(pg_schema)
            # pg.drop_schema_and_recreate(pg_schema, if_exists=True, cascade=True)  # TODO: uncomment on new pydoni release
            logger.info(f'Re-created schema from scratch')
        else:
            # Drop views in the Postgres schema since they may be dependent on tables
            # that require rebuilding. They will all be re-created later
            logger.info(f'Appending new information to {bold(pg_schema)}')
            vw_names = list_view_names(vw_def_dpath)
            for vw_name in vw_names:
                logger.debug(f'Dropping view {bold(vw_name)}')
                view = View(pg_schema=pg_schema, vw_name=vw_name, pg=pg, logger=logger)
                view.drop(if_exists=True, cascade=True)

        #
        # chat.db tables
        #

        logger.info(f'Saving tables to schema "{bold(pg_schema)}"')
        chat_db_extract.save_to_postgres(pg=pg, pg_schema=pg_schema, logger=logger)

        #
        # Custom tables
        #

        build_custom_tables(logger=logger, pg=pg, pg_schema=pg_schema)

        #
        # Chat.db dependent views
        #

        vw_names = list_view_names(vw_def_dpath)
        validate_vw_info(vw_names)
        logger.debug('View .sql files validated and are compatible with vw_info_*.json files')

        logger.info(f'Defining Postgres views that are only dependent on chat.db tables')
        with open(join(vw_dpath, 'view_info_chatdb_dependent.json'), 'r') as f:
            vw_info_chatdb_dependent = json.load(f)

        chatdb_dependent_vw_names = [x for x in vw_names if x in vw_info_chatdb_dependent]

        for vw_name in chatdb_dependent_vw_names:
            view = View(pg_schema=pg_schema, vw_name=vw_name, pg=pg, logger=logger)
            view.create(cascade=True)
            logger.info(f'Defined Postgres:"{bold(pg_schema)}"."{bold(vw_name)}"')

        #
        # Staging tables
        #

        build_staging_tables(pg=pg, pg_schema=pg_schema, logger=logger)

        #
        # Staging table dependent views
        #

    else:
        logger.info('User opted not to save tables to a Postgres database')

    if save_csv is None:
        shutil.rmtree(save_csv_dpath)
        logger.info(f'Removed temporary directory {path(save_csv_dpath)}')

    diff_formatted = pydoni.fmt_seconds(time.time() - start_ts, units='auto', round_digits=2)
    elapsed_time = f"{diff_formatted['value']} {diff_formatted['units']}"
    logger.info(f'iMessage Extractor workflow completed in {elapsed_time}')
