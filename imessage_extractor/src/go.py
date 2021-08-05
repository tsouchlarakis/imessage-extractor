import json
import sqlite3
import pandas as pd
import click
import shutil
import time
import typing
import pathlib
import pydoni
import logging
from .verbosity import print_startup_message, logger_setup, path, bold
from os import makedirs, listdir
from os.path import expanduser, isfile, isdir, splitext, abspath, dirname, join, basename
from .objects import ChatDbExtract, View


vw_dpath = abspath(join(dirname(__file__), 'views'))
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


def generate_global_uid_table(pg: pydoni.Postgres, pg_schema: str, source_table_name: str) -> None:
    """
    Build a table mapping ROWID to a unique identifier for each row in the  message, attachment,
    handle and chat tables. For example, to map message.ROWID to a UID, we would create:

    | ROWID | message_uid         |
    | ----- | ------------------- |
    | 1     | 2632847867257029633 |
    | 2     | 2632847867257029634 |
    | 3     | ...                 |

    In this case, `message_uid` is generated based on the time of insertion, and the previous
    value of `message_uid`. As such, this UID of type BIGINT will never be the same for two
    events in the same table.
    """
    valid_sources = ['message', 'attachment', 'handle', 'chat']
    assert source_table_name in valid_sources, \
        f'`source_table_name` must be one of {str(valid_sources)}'

    sql = f"""
    drop sequence if exists {pg_schema}.map_{source_table_name}_uid_seq;
    create sequence {pg_schema}.map_{source_table_name}_uid_seq;

    create table {pg_schema}.map_{source_table_name}_uid (
        "ROWID" bigint,
        "{source_table_name}_uid" int8 not null default id_generator((nextval('{pg_schema}.map_{source_table_name}_uid_seq'::regclass))::integer)
    );

    insert into {pg_schema}.map_{source_table_name}_uid ("ROWID")
    select t."ROWID"
    from {pg_schema}.{source_table_name} t
    left join {pg_schema}.map_{source_table_name}_uid map
        on t."ROWID" = map."ROWID"
    where t."ROWID" is not null
    and map."ROWID" is null;
    """

    pg.execute(sql)


def list_view_names(vw_dpath: typing.Union[str, pathlib.Path]) -> list:
    """
    Find views in folder.
    """
    return [f.replace('.sql', '') for f in listdir(vw_dpath) if splitext(f)[1] == '.sql']


@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Set logging level to INFO.')
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

@click.command()
def go(chat_db_path,
       verbose,
       save_csv,
       pg_schema,
       pg_credentials) -> None:
    """
    Run the imessage-extractor!
    """
    params = locals()

    # Configure logger
    logging_level = logging.INFO if verbose else logging.ERROR
    logger = logger_setup(name='imessage-extractor', level=logging_level)

    validate_parameters(params)

    # Begin pipeline stopwatch
    start_ts = time.time()

    if verbose:
        print_startup_message(logger)

    # Connect to the local chat.db
    sqlite_con = connect_to_chat_db(chat_db_path)

    # Create a temporary folder to save the extracted data to if the user opted not
    # to save the outputted .csv files to a local folder
    save_csv_dpath = expanduser('~/Desktop/.tmp_imessage_extractor') if save_csv is None else save_csv
    if not isdir(save_csv_dpath):
        makedirs(save_csv_dpath)
        logger.info(f'Created temporary export directory {path(save_csv_dpath)}')

    # Extract metadata for each table in chat.db
    chat_db_extract = ChatDbExtract(sqlite_con, pg_schema, logger)

    # Save tables to .csv files
    logger.info(f'Saving tables to {path(save_csv_dpath)}')
    chat_db_extract.save_to_csv(save_csv_dpath, logger)

    # Refresh target Postgres schema
    if pg_schema is not None:
        # Get Postgres credentials and connect to database
        hostname, port, db_name, pg_user, pw = parse_pg_credentials(pg_credentials)
        pg = pydoni.Postgres(hostname=hostname,
                             port=port,
                             db_name=db_name,
                             pg_user=pg_user,
                             pw=pw)

        logger.info(f'Connected to Postgres database {bold(db_name)} hosted on {bold(hostname + ":" + port)}')

        # Drop all objects in the Postgres schema to rebuild it from scratch
        pg.execute(f'drop schema if exists {pg_schema} cascade')
        pg.execute(f'create schema {pg_schema}')
        # pg.drop_schema_if_exists_and_recreate(pg_schema, cascade=True)  # TODO: uncomment when new version of pydoni released
        logger.info(f'Re-created schema from scratch')

        logger.info(f'Saving tables to schema {bold(pg_schema)}')
        chat_db_extract.save_to_postgres(pg, pg_schema, logger)

        # logger.info('Generating global UIDs for message, attachment, handle and chat tables')
        # core_tables = ['message', 'attachment', 'chat', 'handle']
        # for table_name in core_tables:
        #     generate_global_uid_table(pg=pg, pg_schema=pg_schema, source_table_name=table_name)
        #     logger.info(f'Created sequence {bold(f"{pg_schema}.map_{table_name}_uid_seq")}', arrow='white')
        #     logger.info(f'Created table {bold(f"{pg_schema}.map_{table_name}_uid")}', arrow='white')

        logger.info('Building optional custom tables (if they exist)')
        custom_table_fpaths = [abspath(x) for x in listdir(custom_table_dpath) if not x.startswith('.')]
        any_manual_tabe_defs_exist = len(custom_table_fpaths) > 0
        if any_manual_tabe_defs_exist:
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
            {{column_name1: postgres_data_type1, column_name2: postgres_data_type2, ...}}
            """))

        # Define all Postgres views
        logger.info(f'Defining Postgres views that are only dependent on chat.db tables')
        vw_names = list_view_names(vw_dpath)

        # Views that can be defined after chat.db tables are loaded
        with open(join(dirname(__file__), 'view_info_chat_db_dependent.json'), 'r') as f:
            vw_info_chat_db_dependent = json.load(f)

        # Views that depend on tables created downstream in this pipeline
        with open(join(dirname(__file__), 'view_info_staged_table_dependent.json'), 'r') as f:
            vw_info_staged_table_dependent = json.load(f)

        # Validate that all views defined in vw_info*.json also contain a definition .sql
        # file in the views/ folder, and that all views with definition .sql files in the
        # views/ folder also have a corresponding key in the vw_info*.json file.

        for vw_name in vw_names:
            view_names_all = list(vw_info_chat_db_dependent.keys()) + list(vw_info_staged_table_dependent.keys())
            if vw_name not in view_names_all:
                raise Exception(pydoni.advanced_strip(f"""
                View definition {bold(vw_name)} found at {path(join(vw_dpath, vw_name + ".sql"))}
                but not accounted for in {path("view_info.json")}"""))

        for vw_name in view_names_all:
            if vw_name not in vw_names:
                raise Exception(pydoni.advanced_strip(f"""
                View definition {bold(vw_name)} found in {path("view_info.json")} but not
                accounted for at {path(join(vw_dpath, vw_name + ".sql"))}"""))

        # Define views intelligently. Views may be dependent on other views or tables, and
        # as such, as we cannot simply execute a view definition since a dependency of that
        # view might not exist yet. Instead, we will define views in a manner that ensures
        # that all dependencies for a particular view are created before executing that view's
        # definition.
        #
        # We will iterate through each view in the view_info*.json file, and for each view,
        # check each of its dependencies (if any) to ensure that they exist. If one or more
        # do not, we must navigate to that view in the view_info*.json file and ensure that
        # all of that view's dependencies exist. If one or more do not yet exist, we must then
        # continue navigating down the tree of dependencies until we can create all of them.
        #
        # For example, suppose view A depends on view B and view C, and view B depends on view D.
        # We will attempt to create view A, but it depends on two non-existent views, B and C. We
        # then navigate to view B and find that it depends on view D. We create view D for which
        # all dependencies exist. Then we can create view B. We then check view C, and find that
        # we are able to create it without issue. At this point, all dependencies for view A
        # exist and we can create view A.

        def vw_dependency_exists(pg_schema: str, reference_name: str, pg: pydoni.Postgres) -> bool:
            """
            Determine whether a table or view exists with the given name.
            """
            schema_objects = pg.read_sql(f"""
            select table_name
            from information_schema.tables
            where table_schema = '{pg_schema}'
            """).tolist()

            return reference_name in schema_objects

        def all_references_exist(vw_name: str, vw_info: dict, pg_schema: str, pg: pydoni.Postgres) -> bool:
            """
            Check whether ALL references for a given view exist.
            """
            references = vw_info[vw_name]['reference']
            exists_lst = [vw_dependency_exists(pg_schema, ref, pg) for ref in references]
            return all(exists_lst)

        def get_nonexistent_references(vw_name: str, vw_info: dict, pg_schema: str, pg: pydoni.Postgres) -> list:
            """
            List the nonexistent references for a given view.
            """
            references = vw_info[vw_name]['reference']
            nonexistent_lst = [ref for ref in references if not vw_dependency_exists(pg_schema, ref, pg)]
            return nonexistent_lst

        def smart_create_view(vw_name: str, vw_info: dict, pg_schema: str, pg: pydoni.Postgres) -> None:
            """
            Execute a view definition ONLY IF all references for that view exist. If they do
            not exist, then create them. Each reference might have references themselves, in
            which case we'll need to create those references too.
            """
            if pg.view_exists(pg_schema, vw_name):
                # View already exists, so we can move on
                pass
            else:
                # Main expected branch of function. We need to create the view and all of its
                # references if they don't exist.
                #
                # Because this function is called recursively, it's possible that `vw_name`
                # is a reference of the original view this function was called on. Any reference
                # may be either a table or view. As such, the function all_references_exist()
                # will fail if passed the name of a table, because the table's name is not
                # in view_info.json and it MUST check in that file to get a list of the current
                # view's references. Therefore we must first check whether `vw_name` is a
                # table or a view.
                if vw_name not in vw_info.keys():
                    # This is a table that is not part of the chat.db database. This view
                    # must be moved to a different section of view_info.json.
                    raise Exception(pydoni.advanced_strip(f"""
                    Entity {bold(vw_name)} is not part of the chat.db database, but is required
                    for a view defined in the view_info_chat_db_dependent.json file.
                    Please move any views that depend on {bold(vw_name)} to the
                    view_info_staged_table_dependent.json file, and it will be defined later,
                    after the staged tables are defined."""))
                else:
                    if all_references_exist(vw_name, vw_info, pg_schema, pg):
                        # Create the view
                        vw_obj = View(vw_name=vw_name,
                                      vw_dpath=vw_dpath,
                                      reference=vw_info[vw_name]['reference'],
                                      pg_schema=pg_schema,
                                      pg=pg,
                                      logger=logger)
                        vw_obj.create()
                    else:
                        # Cannot create the view without creating references first
                        # This list must be of length >1 because all_references_exist() returned False
                        nonexistent_references = get_nonexistent_references(vw_name, vw_info, pg_schema, pg)
                        for ref in nonexistent_references:
                            # Create the view. Recursive call to this function
                            smart_create_view(ref, vw_info, pg_schema, pg)


        for vw_name, vw_info_dct in vw_info_chat_db_dependent.items():
            references = vw_info_dct['reference']
            assert isinstance(references, list), \
                f'"references" object for view {bold(vw_name)} in vw_info.json must be a list'

            # Create the view intelligently
            smart_create_view(vw_name, vw_info_chat_db_dependent, pg_schema, pg)
            logger.info(pydoni.advanced_strip(f"""Defined view
            {bold(f'"{pg_schema}"."{vw_name}"')} """), arrow='white')

    else:
        logger.info('User opted not to save tables to a Postgres database')

    if save_csv is None:
        shutil.rmtree(save_csv_dpath)
        logger.info(f'Removed temporary directory {path(save_csv_dpath)}')

    diff_formatted = pydoni.fmt_seconds(time.time() - start_ts, units='auto', round_digits=2)
    elapsed_time = f"{diff_formatted['value']} {diff_formatted['units']}"
    logger.info(f'iMessage Extractor workflow completed in {elapsed_time}')
