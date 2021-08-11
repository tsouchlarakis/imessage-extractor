import logging
import pandas as pd
import pydoni
from ..helpers.objects import StagingTable
from ..helpers.verbosity import bold
from .tables.emoji_text_map import refresh_emoji_text_map
from .tables.message_tokens import refresh_message_tokens
from .tables.stats_by_contact import refresh_stats_by_contact
from .tables.tokens import refresh_tokens


def build_staging_tables(pg: pydoni.Postgres, pg_schema: str, logger: logging.Logger) -> None:
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
        table_obj = StagingTable(
            pg=pg,
            pg_schema=pg_schema,
            table_name=table_name,
            refresh_function=refresh_function,
            logger=logger,
        )

        table_obj.refresh()


def columns_match_expectation(df: pd.DataFrame, table_name: str, columnspec: dict) -> bool:
    """
    Make sure that there is alignment between the columns specified in staging_table_info.json
    and the actual columns in the dataframe about to be inserted.
    """
    expected_columns = sorted([k for k, v in columnspec.items()])
    actual_columns = df.columns

    for col in expected_columns:
        if col not in actual_columns:
            raise KeyError(pydoni.advanced_strip(
                f"""Column {bold(col)} defined in staging_table_info.json
                for table {bold(table_name)} but not in actual dataframe columns
                ({bold(str(df.columns))})"""))

    for col in actual_columns:
        if col not in actual_columns:
            raise KeyError(pydoni.advanced_strip(
                f"""Column {bold(col)} in actual dataframe {bold(table_name)}
                columns ({bold(str(df.columns))}) but not in staging_table_info.json"""))
