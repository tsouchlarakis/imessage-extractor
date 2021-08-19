import re
import logging
import nltk
import pandas as pd
import pydoni
from ...helpers.verbosity import bold
from ..common import columns_match_expectation


def decontracted(string: str) -> str:
    """
    Expand contractions in a string.
    Source: https://stackoverflow.com/questions/19790188/expanding-english-language-contractions-in-python
    """
    string = string.replace('‘', "'").replace('’', "'")  # Standardize smart single quote
    string = string.replace('“', '"').replace('”', '"')  # Standardize smart double quote
    string = string.replace("￼", '')  # Remove ZWNJ character

    # Specific
    string = re.sub(r"won\'t", 'will not', string)
    string = re.sub(r"can\'t", 'can not', string)

    # General
    string = re.sub(r"n\'t", ' not', string)
    string = re.sub(r"\'re", ' are', string)
    string = re.sub(r"\'s", ' is', string)
    string = re.sub(r"\'d", ' would', string)
    string = re.sub(r"\'ll", ' will', string)
    string = re.sub(r"\'t", ' not', string)
    string = re.sub(r"\'ve", ' have', string)
    string = re.sub(r"\'m", ' am', string)

    return string


def clean_message_text(string: str, emoji_lst: list) -> str:
    """
    Apply cleaning on a message text before tokenization, stemming and lemmatization.
    """
    string = re.sub(r'http\S+', '', string)
    string = string.replace('￼', '')
    string = decontracted(string)

    # Surround particular tokens with spaces to ensure they are counted as
    # standalone tokens
    standalone_tokens = emoji_lst + [
        '...',
        '…',
        '—',
        '¡',
        '�',
    ]

    for token in standalone_tokens:
        string = string.replace(token, ' ' + token + ' ')

    string = string.replace(' ️', ' ')

    return re.sub(r'\s+', ' ', string)


def expand_abbreviations(token: str) -> str:
    """
    If a token is an abbreviation for a longer English word, expand it.
    IMPORTANT: Only one-word abbreviations supported. i.e. ATM -> at the moment
    not supported, as a single token 'atm' expands to three unique tokens 'at', 'the', 'moment'
    """
    abb_map = {
        'bc': 'because',
        'ur': 'your',
    }

    if token in abb_map.keys():
        return abb_map[token]
    else:
        return token


def word_tokenize(string: str) -> list:
    """
    Tokenize a string using NLTK, and apply corrections to the NLTK tokenizing algorithm.
    """
    # Preserve tokens that NLTK's tokenize handles undesirably. i.e. gonna -> gon na
    preserve = {
        'gotta': 'got-ta',
        'gonna': 'gon-na',
        'wanna': 'wan-na',
        "'": ' SINGLEQUOTE ',
        '"': ' DOUBLEQUOTE ',
    }
    preserve_inverse = {v.strip(): k for k, v in preserve.items()}

    for s, r in preserve.items():
        string = string.replace(s, r)

    tokens = nltk.tokenize.word_tokenize(string)

    new_tokens = []
    for tok in tokens:
        if tok in preserve_inverse.keys():
            new_tokens.append(preserve_inverse[tok])
        else:
            new_tokens.append(tok)

    return new_tokens


def chunks(lst: list, n: int) -> list:
    """
    Yield successive n-sized chunks from lst.
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def refresh_message_tokens(pg: pydoni.Postgres,
                           pg_schema: str,
                           table_name: str,
                           columnspec: dict,
                           logger: logging.Logger) -> None:
    """
    Parse messages into tokens and append to message tokens table, for messages that have
    not already been parsed into tokens.
    """
    logger.info(f'Refreshing table "{bold(table_name)}"', arrow='yellow')

    batch_size = None
    logger.debug(f'Batch size: {bold(batch_size)}')

    if pg.table_exists(pg_schema, table_name):
        # Filter out messages that are already in the message <> tokens mapping table if it exists
        rebuild = False
        join_clause = f'left join {pg_schema}.{table_name} t on m.message_id = t.message_id'
        and_condition = 'and t.message_id is null  -- Not in existing message <> tokens map'
    else:
        rebuild = True
        join_clause = ''
        and_condition = ''

    sql = f"""
    select m.message_id, m."text"
    from {pg_schema}.message_vw m
    {join_clause}
    where m.is_text = true  -- Not an emote reaction, attachment, or other message with no text
      {and_condition}
    order by m.message_id
    """
    message = pg.read_sql(sql)
    logger.debug(f'Tokenizing {len(message)} messages')

    if isinstance(batch_size, int):
        target_indices = list(chunks(list(message.index), batch_size))
    else:
        target_indices = [list(message.index)]

    emoji_lst = pg.read_sql(f'select emoji from {pg_schema}.emoji_text_map').squeeze().tolist()

    total_tokens_inserted = 0
    message_tokens = pd.DataFrame(columns=[k for k, v in columnspec.items()])
    for i, targets in enumerate(target_indices):
        if isinstance(batch_size, int):
            logger.debug(f'Tokenizing batch {i + 1} of {len(target_indices)}')

        message_subset = message.loc[message.index.isin(targets)]

        for i, row in message_subset.iterrows():
            text = clean_message_text(row['text'], emoji_lst)
            tokens = word_tokenize(text)
            tokens = [expand_abbreviations(x) for x in tokens]
            total_tokens_inserted += len(tokens)

            pos_tags = nltk.pos_tag(tokens)
            pos_tags_simple = [(word, nltk.tag.map_tag('en-ptb', 'universal', pos_tag)) for word, pos_tag in pos_tags]
            iterator = enumerate(zip(tokens,
                                     [y for x, y in pos_tags],
                                     [y for x, y in pos_tags_simple]))

            for i, (tok, pos, pos_simple) in iterator:
                value_lst = [row['message_id'], i+1, tok, pos, pos_simple]
                try:
                    message_tokens.loc[len(message_tokens)+1] = value_lst
                except Exception:
                    raise ValueError(pydoni.advanced_strip(f"""Incompatible column specification
                    in staging_table_info.json for table {bold(table_name)}. Columns should be:
                    [message_id, token_idx, token, pos, pos_simple]."""))

    # Make doubly sure that we're not attempting to insert any duplicate records (in case
    # the message_tokens table might have changed from the start to finish of this command)
    if pg.table_exists(pg_schema, table_name):
        existing_message_tokens = pg.read_table(pg_schema, table_name)
        message_tokens = message_tokens.loc[~message_tokens['message_id'].isin(existing_message_tokens['message_id'])]

    columns_match_expectation(message_tokens, table_name, columnspec)
    message_tokens.to_sql(name=table_name,
                          con=pg.dbcon,
                          schema=pg_schema,
                          index=False,
                          if_exists='append')

    if len(message_tokens):
        participle = 'Rebuilt' if rebuild else 'Appended'
        logger.info(f'{participle} table "{bold(table_name)}", shape: {message_tokens.shape}', arrow='yellow')
    else:
        logger.info(f'No messages to add to table "{bold(table_name)}"', arrow='yellow')
