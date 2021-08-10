import emoji
import logging
import pandas as pd
import pydoni
from ....verbosity import bold
from ..common import columns_match_expectation


def refresh_emoji_text_map(pg: pydoni.Postgres,
                           pg_schema: str,
                           table_name: str,
                           columnspec: dict,
                           logger: logging.Logger) -> None:
    """
    Refresh table emoji_text_map.
    """
    logger.info(f'Refreshing staging table "{bold(pg_schema)}"."{bold(table_name)}"', arrow='white')

    emoji_table = pd.DataFrame(emoji.UNICODE_EMOJI['en'], index=[0])
    emoji_table = emoji_table.T.reset_index().rename(columns={'index': 'emoji', 0: 'plain_text'})

    columns_match_expectation(emoji_table, table_name, columnspec)
    emoji_table.to_sql(name=table_name,
                       con=pg.dbcon,
                       schema=pg_schema,
                       index=False,
                       if_exists='replace')

    logger.info(f'Rebuilt "{bold(pg_schema)}"."{bold(table_name)}", shape: {emoji_table.shape}', arrow='white')
