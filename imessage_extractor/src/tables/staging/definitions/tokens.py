import logging
import nltk
import pydoni
import string
from nltk.corpus import stopwords
import typing
from ....verbosity import bold
from ..common import columns_match_expectation


def is_punctuation(token):
    """
    Determine whether a token is punctuation.
    """
    return token in string.punctuation + '’‘“”``'


def refresh_tokens(pg: pydoni.Postgres,
                   pg_schema: str,
                   table_name: str,
                   columnspec: dict,
                   references: typing.Union[list, None],
                   logger: logging.Logger):
    """
    Map each unique token to descriptor columns (stem, lemma, length, language, etc.).
    """
    logger.info(f'Refreshing staging table "{bold(pg_schema)}"."{bold(table_name)}"')

    message_tokens_table_name = 'message_tokens'

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
    logger.info(f'Gathering token data for {len(new_tokens)} new, unique tokens')

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

    columns_match_expectation(new_tokens, table_name, columnspec)
    new_tokens.to_sql(table_name, pg.dbcon, schema=pg_schema, index=False, if_exists='append')

    n_new_tokens_str = len(new_tokens) if len(new_tokens) > 0 else 'no'
    logger.info(f'Built "{bold(pg_schema)}"."{bold(table_name)}", shape: {new_tokens.shape}')
