import logging

import emoji
import nltk
import pydoni
import string
from ....verbosity import bold
from ..common import columns_match_expectation
from nltk.corpus import stopwords


def is_punctuation(token):
    """
    Determine whether a token is punctuation.
    """
    return token in string.punctuation + '’‘“”``'


def is_emoji(token):
  """
  Determine whether a token is an emoji.
  """


def refresh_tokens(pg: pydoni.Postgres,
                   pg_schema: str,
                   table_name: str,
                   columnspec: dict,
                   logger: logging.Logger):
    """
    Map each unique token to descriptor columns (stem, lemma, length, language, etc.).
    """
    logger.info(f'Refreshing staging table "{bold(pg_schema)}"."{bold(table_name)}"', arrow='white')

    message_tokens_table_name = 'message_tokens'
    emoji_text_map_table_name = 'emoji_text_map'

    if pg.table_exists(pg_schema, table_name):
      # Filter out messages that are already in the message <> emoji mapping table if
      # it exists
      join_clause = f"""
      left join {pg_schema}.{table_name} e
            on lower(m."token") = lower(e."token")
           and e."token" is null  -- Not in existing tokens table"""
    else:
      join_clause = ''

    new_tokens = pg.read_sql(f"""
    select distinct lower(m.token) as "token"
    from {pg_schema}.{message_tokens_table_name} m
    {join_clause}
    """, simplify=False)  # Returns a dataframe with one column
    logger.debug(f'Gathering information by token for {len(new_tokens)} new, unique tokens')

    emoji_text_map = pg.read_table(pg_schema, emoji_text_map_table_name)[['emoji']]
    emoji_text_map['is_emoji'] = True

    if len(new_tokens) > 0:
        stops = stopwords.words('english')
        lemmatizer = nltk.stem.WordNetLemmatizer()
        stemmer = nltk.stem.PorterStemmer()

        column_apply_function_map = {
            'length': len,
            'stem': stemmer.stem,
            'lemma': lemmatizer.lemmatize,
            'is_english_stopword': lambda tok: tok in stops,
            'is_punct': is_punctuation,
        }

        for col, fun in column_apply_function_map.items():
            new_tokens[col] = new_tokens['token'].apply(fun)
            logger.debug(f'Applied aggregations to column {bold(col)}')

        # Add emoji information
        new_tokens = new_tokens.merge(emoji_text_map[['emoji']], left_on='token', right_on='emoji', how='left')
        new_tokens['is_emoji'].fillna(False, inplace=True)

        columns_match_expectation(new_tokens, table_name, columnspec)
        new_tokens.to_sql(name=table_name,
                          con=pg.dbcon,
                          schema=pg_schema,
                          index=False,
                          if_exists='append')

        logger.info(f'Built "{bold(pg_schema)}"."{bold(table_name)}", shape: {new_tokens.shape}')

    else:
        logger.info(f'No new tokens to add to "{bold(pg_schema)}"."{bold(table_name)}"', arrow='white')
