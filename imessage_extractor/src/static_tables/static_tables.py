import json
import logging
import sqlite3
import pandas as pd
from imessage_extractor.src.helpers.config import WorkflowConfig
from imessage_extractor.src.helpers.utils import strip_ws
from imessage_extractor.src.helpers.verbosity import bold, path, code
from os import listdir
from os.path import isfile, join, abspath


class StaticTable(object):
    """
    Store information and operations on user-defined static tables.
    """
    def __init__(self,
                 table_name: str,
                 sqlite_con: sqlite3.Connection,
                 logger: logging.Logger,
                 cfg: 'WorkflowConfig') -> None:
        self.table_name = table_name
        self.sqlite_con = sqlite_con
        self.logger = logger
        self.cfg = cfg

        self.csv_fpath = join(self.cfg.dir.static_table_data, table_name + '.csv')
        if not isfile(self.csv_fpath):
            raise FileNotFoundError(strip_ws(
                f"""static table {bold(table_name)} data .csv file expected at
                {path(self.csv_fpath)} but not found"""))
        else:
            self.df = pd.read_csv(self.csv_fpath)

        with open(self.cfg.file.static_table_info, 'r') as json_file:
            schemas = json.load(json_file)
            if table_name not in schemas:
                raise KeyError(strip_ws(
                    f"""Attempting to define static table "{self.table_name}",
                    the table data .csv file exists at "{self.csv_fpath}" but a column
                    specification does not exist in "{self.cfg.file.static_table_info}" for
                    that table. Add a key in that JSON file with the same name as the .csv file
                    with a corresponding value that is a dictionary with a name: dtype pair
                    for each column in the .csv file. For example, if the .csv file has
                    columns ['id', 'message'], then add an entry to that JSON file:
                    "{table_name}": {{"id": "INTEGER", "message": "TEXT"}}"""))
            else:
                table_schema = schemas[self.table_name]

        self._validate_columns(table_schema)

    def _validate_columns(self, table_schema: dict):
        """
        Now that we know there exists a .csv and a JSON key for each static table,
        we can proceed with validating that the table schemas defined in the JSON file
        are compatible with the data contained the corresponding .csv files.
        """
        for col_name, col_dtype in table_schema.items():
            # Validate that column specified in JSON config actually exists in the .csv file
            if col_name not in self.df.columns:
                raise KeyError(strip_ws(
                    f'''Column "{col_name}" specified in "{self.cfg.file.static_table_info}"
                    but does not exist in "{self.csv_fpath}"'''))

        for col_name in self.df.columns:
            # Validate that the column in the .csv file actually exists in the JSON config
            if col_name not in table_schema:
                raise KeyError(strip_ws(
                    f'''Column "{col_name}" exists in "{self.csv_fpath}" but
                    has no definition in the key "{self.table_name}" in the configuration file
                    "{self.cfg.file.static_table_info}"'''))

    def save_to_sqlite(self):
        """
        Save static table to SQLite by overwriting the table if it exists.
        """
        self.df.to_sql(name=self.table_name,
                       con=self.sqlite_con,
                       schema='main',
                       index=False,
                       if_exists='replace')


def build_static_tables(sqlite_con: sqlite3.Connection,
                        logger: logging.Logger,
                        cfg: WorkflowConfig) -> None:
    """
    Build static, user-maintained tables, the information for which is stored in the
    'static_tables' folder.
    """
    expected_contacts_csv_fpath = join(cfg.dir.static_table_data, 'contacts.csv')
    expected_contacts_ignored_csv_fpath = join(cfg.dir.static_table_data, 'contacts_ignored.csv')
    expected_contacts_manual_csv_fpath = join(cfg.dir.static_table_data, 'contacts_manual.csv')

    with open(join(cfg.dir.static_tables, 'static_table_info.json')) as f:
        static_table_info = json.load(f)

    if not isfile(expected_contacts_csv_fpath):
        columns = [k for k, v in static_table_info['contacts'].items()]
        with open(expected_contacts_csv_fpath, 'w') as f:
            f.write(','.join(columns))

    if not isfile(expected_contacts_ignored_csv_fpath):
        columns = [k for k, v in static_table_info['contacts_ignored'].items()]
        with open(expected_contacts_ignored_csv_fpath, 'w') as f:
            f.write(','.join(columns))

    if not isfile(expected_contacts_manual_csv_fpath):
        columns = [k for k, v in static_table_info['contacts_manual'].items()]
        with open(expected_contacts_manual_csv_fpath, 'w') as f:
            f.write(','.join(columns))

    static_table_fpaths = [abspath(x) for x in listdir(cfg.dir.static_table_data) if not x.startswith('.')]

    if len(static_table_fpaths) > 0:
        logger.info('Build Static Tables', bold=True)
        with open(cfg.file.static_table_info, 'r') as json_file:
            static_table_info = json.load(json_file)

            for table_name, table_info in static_table_info.items():
                table_object = StaticTable(table_name=table_name,
                                           sqlite_con=sqlite_con,
                                           logger=logger,
                                           cfg=cfg)

                table_object.save_to_sqlite()
                logger.info(f'Created table {code(table_name)}', arrow='black')

    else:
        logger.warning(strip_ws(
            f"""No static table data exists, so no static tables were created. This is perfectly
            okay and does not affect the running of this pipeline, however know that if you'd
            like to add a table to this pipeline, you can manually create a .csv file and
            place it in the {path(cfg.dir.static_table_data)} folder.
            The resulting table will be dropped and re-created with each run of this pipeline,
            and will be named identically to how you choose to name the .csv file. NOTE:
            each .csv file must be accompanied by a key:value pair in static_table_info.json
            that specifies the table schema, or else an error will be thrown. That
            table schema should be in the format:
            {{column_name1: data_type1, column_name2: data_type2, ...}}"""))
