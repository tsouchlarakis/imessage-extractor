import datetime
import pydoni
import logging
import pandas as pd
from ....verbosity import bold
from ..common import columns_match_expectation


def refresh_stats_by_contact(pg: pydoni.Postgres,
                             pg_schema: str,
                             table_name: str,
                             columnspec: dict,
                             logger: logging.Logger) -> None:
    """
    Maintain a table containing aggregated statistics by contact name (i.e. one row
    per contact name, containing stats such as total messages, earliest message date,
    most recent message date, etc.).
    """
    # Get message stats per contact
    logger.info(f'Refreshing staging table "{bold(pg_schema)}"."{bold(table_name)}"', arrow='yellow')

    message = pg.read_sql(f"""
    select message_id
           , contact_name
           , dt
           , "text"
           , n_characters
           , n_words
           , case when is_from_me = true then 'from_me'
                  when is_from_me = false then 'from_them'
                  else null
             end as from_me_cat
    from {pg_schema}.message_vw
    where is_text = true
      and message_id is not null
    """)

    aggregations = {
        'message_id': pd.Series.nunique,
        'n_characters': sum,
        'n_words': sum,
        'dt': [min, max, pd.Series.nunique],
    }

    contact_name = message.groupby('contact_name').agg(aggregations)
    contact_name = pydoni.collapse_df_columns(contact_name)

    contact_name_from_me = message.groupby(['contact_name', 'from_me_cat']).agg(aggregations)
    contact_name_from_me = pydoni.collapse_df_columns(contact_name_from_me).reset_index()

    value_columns = [x for x in contact_name_from_me.columns if x not in ['contact_name', 'from_me_cat']]
    contact_name_from_me = contact_name_from_me.pivot(index='contact_name',
                                                      columns='from_me_cat',
                                                      values=value_columns)

    contact_name_from_me = pydoni.collapse_df_columns(contact_name_from_me)

    column_rename_map = {
        'message_uid_nunique': 'n_messages',
        'n_characters_sum': 'n_characters',
        'n_words_sum': 'n_words',
        'message_dt_min': 'first_message_dt',
        'message_dt_max': 'last_message_dt',
        'message_dt_nunique': 'n_active_days',
    }

    for x, y in column_rename_map.items():
        new_columns = []
        for c in contact_name.columns:
            if x in c:
                new_columns.append(c.replace(x, y))
            else:
                new_columns.append(c)

        contact_name.columns = new_columns

        new_columns = []
        for c in contact_name_from_me.columns:
            if x in c:
                new_columns.append(c.replace(x, y))
            else:
                new_columns.append(c)

        contact_name_from_me.columns = new_columns

    logger.info('Computing top token statistics by contact')
    contact_top_tokens_and_emojis = pg.read_sql(f"""
    with t1 as (
        select *, row_number() over(
                    partition by contact_name, case when is_emoji = true then 1 else 0 end
                    order by n_token_uses desc) as r
        from {pg_schema}.contact_token_usage_vw
    )

    , t2 as (
        select *
        from t1
        where r = 1
    )

    , t3 as (
        select contact_name
               , "token" as top_token
               , n_token_uses as n_top_token_uses
               , n_messages_where_token_used as n_messages_where_top_token_used
               , first_use_dt as top_token_first_use_dt
               , last_use_dt as top_token_last_use_dt
        from t2
        where is_emoji = true
    )

    , t4 as (
        select t2.contact_name
               , t2."token" as top_emoji
               , e.plain_text as top_emoji_text
               , t2.n_token_uses as n_top_emoji_uses
               , t2.n_messages_where_token_used as n_messages_where_top_emoji_used
               , t2.first_use_dt as top_emoji_first_use_dt
               , t2.last_use_dt as top_emoji_last_use_dt
        from t2
        left join {pg_schema}.emoji_text_map e
               on t2."token" = e.emoji
        where is_emoji = false
    )

    select *
    from t3
    left join t4
    using (contact_name)
    """)

    # Combine stats into a single dataframe
    logger.info('Computing dependent aggregated columns')

    stats = (contact_name
             .merge(contact_name_from_me, on='contact_name', how='left')
             .merge(contact_top_tokens_and_emojis, on='contact_name', how='left')).copy()

    #
    # Compute dependent columns
    #

    date_columns = [
        'first_message_dt',
        'first_message_dt_from_me',
        'first_message_dt_from_them',
        'last_message_dt',
        'last_message_dt_from_me',
        'last_message_dt_from_them',
        'top_emoji_first_use_dt',
        'top_emoji_last_use_dt',
        'top_token_first_use_dt',
        'top_token_last_use_dt',
    ]
    for c in date_columns:
        stats[c] = pd.to_dttime(stats[c])

    stats['n_days_from_first_to_last_message_dt'] = \
        (stats['last_message_dt'] - stats['first_message_dt']).apply(lambda x: x.days)

    stats['pct_active_days'] = \
        1.0 * stats['n_active_days'] / stats['n_days_from_first_to_last_message_dt']

    stats['pct_messages_from_me'] = \
        1.0 * stats['n_messages_from_me'] / stats['n_messages']

    stats['pct_messages_from_them'] = \
        1.0 * stats['n_messages_from_them'] / stats['n_messages']

    stats['n_days_since_last_message'] = \
        (datetime.datetime.now() - stats['last_message_dt']).apply(lambda x: x.days)

    stats['n_days_since_last_reply_from_me'] = \
        (datetime.datetime.now() - stats['last_message_dt_from_me']).apply(lambda x: x.days)

    stats['n_days_since_last_reply_from_them'] = \
        (datetime.datetime.now() - stats['last_message_dt_from_them']).apply(lambda x: x.days)

    columns_ordered = ['contact_name'] + sorted([x for x in stats.columns if x != 'contact_name'])
    stats = stats[columns_ordered]

    columns_match_expectation(stats, table_name, columnspec)
    stats.to_sql(name=table_name,
                 con=pg.dbcon,
                 schema=pg_schema,
                 index=False,
                 if_exists='replace')

    logger.info(f'Rebuilt "{bold(pg_schema)}"."{bold(table_name)}", shape: {stats.shape}', arrow='yellow')
