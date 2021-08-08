import pydoni
import logging
from ...objects import StagedTable
from .definitions.emoji_text_map import refresh_emoji_text_map
from .definitions.message_tokens import refresh_message_tokens
from .definitions.message_emoji_map import refresh_message_emoji_map


def build_staged_tables(pg: pydoni.Postgres, pg_schema: str, logger: logging.Logger) -> None:
    """
    Build each staged table sequentially. Import each table's refresh function into
    this script, and add an entry in the `refresh_map` for each table that you'd
    like to refresh.
    """
    logger.info('Building staged tables')

    refresh_map = dict(
        emoji_text_map=refresh_emoji_text_map,
        # message_tokens=refresh_message_tokens,
        message_emoji_map=refresh_message_emoji_map,
    )

    for table_name, refresh_function in refresh_map.items():
        table_obj = StagedTable(
            pg_schema=pg_schema,
            table_name=table_name,
            refresh_function=refresh_function,
        )

        table_obj.refresh(
            pg=pg,
            pg_schema=pg_schema,
            table_name=table_name,
            columnspec=table_obj.columnspec,
            logger=logger,
        )