import click
import json
import logging
import pandas as pd
from ..helpers.config import WorkflowConfig
from ..helpers.verbosity import bold, path
from os import listdir
from os.path import isfile, join, splitext, basename, abspath
from pydoni import advanced_strip, Postgres


class CustomTable(object):
    """
    Store information and operations on user-defined custom tables.
    """
    def __init__(self,
                 table_name: str,
                 pg: Postgres,
                 logger: logging.Logger,
                 cfg: 'WorkflowConfig') -> None:
        self.table_name = table_name
        self.pg = pg
        self.logger = logger
        self.cfg = cfg

        self.csv_fpath = join(self.cfg.dir.custom_table_data, table_name + '.csv')
        if not isfile(self.csv_fpath):
            raise FileNotFoundError(advanced_strip(
                f"""Custom table {bold(table_name)} data .csv file expected at
                {path(self.csv_fpath)} but not found"""))
        else:
            self.df = pd.read_csv(self.csv_fpath)

        with open(self.cfg.file.custom_table_info, 'r') as json_file:
            schemas = json.load(json_file)
            if table_name not in schemas:
                raise KeyError(advanced_strip(
                    f"""Attempting to define custom table "{self.table_name}",
                    the table data .csv file exists at "{self.csv_fpath}" but a column
                    specification does not exist in "{self.cfg.file.custom_table_info}" for
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
        Now that we know there exists a .csv and a JSON key for each custom table,
        we can proceed with validating that the table schemas defined in the JSON file
        are compatible with the data contained the corresponding .csv files.
        """
        for col_name, col_dtype in table_schema.items():
            # Validate that column specified in JSON config actually exists in the .csv file
            if col_name not in self.df.columns:
                raise KeyError(advanced_strip(
                    f'''Column "{col_name}" specified in "{self.cfg.file.custom_table_info}"
                    but does not exist in "{self.csv_fpath}"'''))

        for col_name in self.df.columns:
            # Validate that the column in the .csv file actually exists in the JSON config
            if col_name not in table_schema:
                raise KeyError(advanced_strip(
                    f'''Column "{col_name}" exists in "{self.csv_fpath}" but
                    has no definition in the key "{self.table_name}" in the configuration file
                    "{self.cfg.file.custom_table_info}"'''))

    def save_to_postgres(self):
        """
        Save custom table to Postgres by overwriting the table if it exists.
        """
        self.df.to_sql(name=self.table_name,
                       con=self.pg.dbcon,
                       schema=self.cfg.pg_schema,
                       index=False,
                       if_exists='replace')


def build_custom_tables(pg: Postgres,
                        logger: logging.Logger,
                        cfg: WorkflowConfig) -> None:
    """
    Build custom, user-maintained tables, the information for which is stored in the
    'custom_tables' folder.
    """
    custom_table_fpaths = [abspath(x) for x in listdir(cfg.dir.custom_table_data) if not x.startswith('.')]

    if len(custom_table_fpaths) > 0:
        logger.info('Building optional custom tables')
        schemas_fpath = join(cfg.dir.custom_table_data, 'custom_table_schemas.json')

        with open(cfg.file.custom_table_info, 'r') as json_file:
            custom_table_info = json.load(json_file)

            for table_name, table_info in custom_table_info.items():
                table_object = CustomTable(table_name=table_name,
                                           pg=pg,
                                           logger=logger,
                                           cfg=cfg)

                table_object.save_to_postgres()
                logger.info(f'Rebuilt table "{bold(table_name)}"', arrow='magenta')

    else:
        logger.warning(advanced_strip(
            f"""No custom table data exists, so no custom tables were created. This is perfectly
            okay and does not affect the running of this pipeline, however know that if you'd
            like to add a table to this pipeline, you can manually create a .csv file and
            place it in the {path(cfg.dir.custom_table_data)} folder.
            The resulting table will be dropped and re-created with each run of this pipeline,
            and will be named identically to how you choose to name the .csv file. NOTE:
            each .csv file must be accompanied by a key:value pair in custom_table_info.json
            that specifies the table schema, or else an error will be thrown. That
            table schema should be in the format:
            {{column_name1: postgres_data_type1, column_name2: postgres_data_type2, ...}}"""))
