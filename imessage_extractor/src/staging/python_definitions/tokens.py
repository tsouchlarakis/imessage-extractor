import logging
import nltk
import string
from imessage_extractor.src.chatdb.chatdb import ChatDb
from imessage_extractor.src.helpers.verbosity import bold
from imessage_extractor.src.staging.common import columns_match_expectation
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


def refresh_tokens(chatdb: 'ChatDb',
                   table_name: str,
                   columnspec: dict,
                   logger: logging.Logger):
    """
    Map each unique token to descriptor columns (stem, lemma, length, language, etc.).
    """
    logger.info(f'Refreshing table "{bold(table_name)}"', arrow='yellow')

    message_tokens_table_name = 'message_tokens'
    emoji_text_map_table_name = 'emoji_text_map'

    if chatdb.table_exists(table_name):
        # Filter out messages that are already in the message <> emoji mapping table if
        # it exists
        rebuild = False
        join_clause = f'left join {table_name} e on lower(m."token") = lower(e."token")'
        where_clause = 'where e."token" is null  -- Not in existing tokens table'
    else:
        rebuild = True
        join_clause = ''
        where_clause = ''

    query_sql = f"""
    select distinct lower(m.token) as "token"
    from {message_tokens_table_name} m
    {join_clause}
    {where_clause}"""
    new_tokens = chatdb.read_sql(query_sql, simplify=False)  # Returns a dataframe with one column
    logger.debug(f'Gathering information by token for {len(new_tokens)} new, unique tokens')

    emoji_text_map = chatdb.read_table(emoji_text_map_table_name)[['emoji']]
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
            logger.debug(f'Applied aggregations to compute column {bold(col)}')

        # Add emoji information
        emoji_text_map['is_emoji'] = True
        new_tokens = new_tokens.merge(emoji_text_map[['emoji', 'is_emoji']], left_on='token', right_on='emoji', how='left')
        new_tokens['is_emoji'].fillna(False, inplace=True)
        new_tokens = new_tokens.drop('emoji', axis=1).sort_values('token')

        columns_match_expectation(new_tokens, table_name, columnspec)
        new_tokens.to_sql(name=table_name,
                          con=chatdb.sqlite_con,
                          schema='main',
                          index=False,
                          if_exists='append')

        participle = 'Rebuilt' if rebuild else 'Appended'
        logger.info(f'{participle} table "{bold(table_name)}", shape: {new_tokens.shape}', arrow='yellow')

    else:
        logger.info(f'No new tokens to add to table "{bold(table_name)}"', arrow='yellow')
