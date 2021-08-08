import pydoni
import logging
from ...objects import StagingTable
from .definitions.emoji_text_map import refresh_emoji_text_map
from .definitions.message_tokens import refresh_message_tokens
from .definitions.message_emoji_map import refresh_message_emoji_map
from .definitions.tokens import refresh_tokens
from .definitions.stats_by_contact import refresh_stats_by_contact


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
        # message_emoji_map=refresh_message_emoji_map,
        # tokens=refresh_tokens,
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