import emoji
import logging
import pandas as pd
from imessage_extractor.src.chatdb.chatdb import ChatDb
from imessage_extractor.src.helpers.verbosity import bold, code
from imessage_extractor.src.staging.common import columns_match_expectation


def refresh_emoji_text_map(chatdb: 'ChatDb',
                           table_name: str,
                           columnspec: dict,
                           logger: logging.Logger) -> None:
    """
    Refresh table emoji_text_map.
    """
    logger.debug(f'Refreshing table "{bold(table_name)}"', arrow='yellow')

    emoji_table = pd.DataFrame(emoji.UNICODE_EMOJI['en'], index=[0])
    emoji_table = emoji_table.T.reset_index().rename(columns={'index': 'emoji', 0: 'plain_text'})

    columns_match_expectation(emoji_table, table_name, columnspec)
    emoji_table.to_sql(name=table_name,
                       con=chatdb.sqlite_con,
                       schema='main',
                       index=False,
                       if_exists='replace')

    logger.info(f'Built table {code(table_name)}', arrow='black')
