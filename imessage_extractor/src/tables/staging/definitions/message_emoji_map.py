import pandas as pd
import pydoni
import typing
import logging
from ....verbosity import bold


def refresh_message_emoji_map(pg: pydoni.Postgres,
                              pg_schema: str,
                              table_name: str,
                              columnspec: dict,
                              references: typing.Union[list, None],
                              logger: logging.Logger) -> None:
    """
    Refresh or rebuild a table in Postgres mapping messages to a boolean columns indicating
    whether that message contains an emoji.
    """
    logger.info(f'Refreshing staging table "{bold(pg_schema)}"."{bold(table_name)}"')

    emoji_text_map_table_name = 'emoji_text_map'
    emoji_table = pg.read_table(pg_schema, emoji_text_map_table_name)
    emoji_lst = emoji_table['emoji'].tolist()
    assert len(emoji_lst) > 0, f'Table {bold(pg_schema + "." + emoji_text_map_table_name)} is malformed'
    logger.debug(f'Fetched {len(emoji_lst)} unique emojis from {bold(pg_schema)}.{bold(emoji_text_map_table_name)}')

    if pg.table_exists(pg_schema, table_name):
      # Filter out messages that are already in the message <> emoji mapping table if
      # it exists
      join_clause = f"""
      left join {pg_schema}.{table_name} e
            on m.message_id = e.message_id
           and e.message_id is null  -- Not in existing message <> emoji map"""
    else:
      join_clause = ''

    message_sql = f"""
    select m.message_id, m."text"
    from {pg_schema}.message_vw m
    {join_clause}
    where m.is_text = true
    """
    logger.debug(message_sql)
    messages = pg.read_sql(message_sql)

    message_emoji_map = pd.DataFrame(columns=[k for k, v in columnspec.items()])
    for i, row in messages.iterrows():
        msg_has_emoji = any([em in row['text'] for em in emoji_lst])
        message_emoji_map.loc[len(message_emoji_map) + 1] = [row['message_id'], msg_has_emoji]

    logger.debug('Created message_emoji_map Pandas dataframe. Inserting into Postgres...')

    message_emoji_map.to_sql(name=table_name,
                             con=pg.dbcon,
                             schema=pg_schema,
                             index=False,
                             if_exists='append')

    logger.info(f'Built "{bold(pg_schema)}"."{bold(table_name)}", shape: {message_emoji_map.shape}')
