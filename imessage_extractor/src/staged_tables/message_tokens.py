import logging
import nltk
import pandas as pd
from tqdm import tqdm


def decontracted(string):
    """
    Expand contractions in a string.
    Source: https://stackoverflow.com/questions/19790188/expanding-english-language-contractions-in-python
    """
    import re

    string = string.replace('‘', "'").replace('’', "'")  # Standardize smart single quote
    string = string.replace('“', '"').replace('”', '"')  # Standardize smart double quote
    string = string.replace("￼", '')  # Remove ZWNJ character

    # Specific
    string = re.sub(r"won\'t", "will not", string)
    string = re.sub(r"can\'t", "can not", string)

    # General
    string = re.sub(r"n\'t", " not", string)
    string = re.sub(r"\'re", " are", string)
    string = re.sub(r"\'s", " is", string)
    string = re.sub(r"\'d", " would", string)
    string = re.sub(r"\'ll", " will", string)
    string = re.sub(r"\'t", " not", string)
    string = re.sub(r"\'ve", " have", string)
    string = re.sub(r"\'m", " am", string)

    return string


def clean_message_text(string, emoji_lst):
    """
    Apply cleaning on a message text before tokenization, stemming and lemmatization.
    """
    import re
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


def expand_abbreviations(token):
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


def word_tokenize(string):
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


def chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def refresh_message_tokens(pg: pydoni.Postgres,
                           pg_schema: str,
                           table_name: str,
                           limit: int,
                           batch_size: int,
                           message_tokens_table,
                           logger: logging.Logger):
    """
    Parse messages into tokens and append to message tokens table, for messages that have
    not already been parsed into tokens.
    """
    sql = f"""
    select message_id
           , source
           , "ROWID" :: int as "ROWID"
           , "text"
    from imessage.message_vw
    where is_text = true  -- Not a 'like' or other emote reaction
      and message_uid not in (select distinct message_uid from {pg_schema}.{table_name})  -- Not already in target table
    order by message_date asc
    """
    message = pg.read_sql(sql)
    logger.info(f'Total eligible messages: {len(message)}')

    if isinstance(limit, int):
        message = message.head(limit)

    if isinstance(batch_size, int):
        is_batched = True
        target_indices = list(chunks(list(message.index), batch_size))
        n_batches = len(target_indices)
        plural = '' if n_batches == 1 else 'es'
        logger.info(f'Expanding {n_batches} batch{plural} of {batch_size} messages ({len(message)} total) to one-row-per-token format')
    else:
        is_batched = False
        target_indices = [list(message.index)]
        logger.info(f'Expanding {len(message)} messages to one-row-per-token format')

    emoji_lst = pg.read_sql('select emoji from imessage.emoji_text_map').squeeze().tolist()

    total_tokens_inserted = 0
    for i, targets in enumerate(target_indices):
        message_tokens = pd.DataFrame(columns=[x for x, y in message_tokens_table.columnspec])
        message_subset = message.loc[message.index.isin(targets)]

        if is_batched:
            logger.info(f'Executing batch {i + 1} of {n_batches}')

        if vb.verbose:
            vb.pbar = tqdm(total=len(message_subset), unit='message')

        for i, row in message_subset.iterrows():
            text = clean_message_text(row['text'], emoji_lst)
            tokens = word_tokenize(text)
            tokens = [expand_abbreviations(x) for x in tokens]
            total_tokens_inserted += len(tokens)

            pos_tags = nltk.pos_tag(tokens)
            pos_tags_simple = [(word, nltk.tag.map_tag('en-ptb', 'universal', pos_tag)) for word, pos_tag in pos_tags]
            iterator = enumerate(zip(tokens,
                                     [y for x, y in pos_tags],
                                     [y for x, y in pos_tags_simple],
                                    ))

            for i, (tok, pos, pos_simple) in iterator:
                message_tokens.loc[len(message_tokens)+1] = [
                    row['message_uid'],
                    row['source'],
                    row['ROWID'],
                    i + 1,
                    tok,
                    pos,
                    pos_simple,
                ]

            vb.pbar_update(1)

        vb.pbar_close()

        # Make doubly sure that we're not attempting to insert any duplicate records (in case
        # the message_tokens table might have changed from the start to finish of this command)
        existing_message_tokens = pg.read_table(pg_schema, table_name)
        message_tokens = message_tokens.loc[~message_tokens['message_uid'].isin(existing_message_tokens['message_uid'])]
        if not dry_run:
            message_tokens.to_sql(name=table_name,
                                  con=pg.dbcon,
                                  schema=pg_schema,
                                  index=False,
                                  if_exists='append')

        logger.info(f"Inserted {len(message_tokens)} tokens to {pg_schema}.{table_name}")

    # Ideally we'd want n_messages_parsed and n_messages_inserted to be equal. This would
    # indicate that no records to the message_tokens table were inserted by a different
    # process during the running of this command
    logger.info(f'Total messages processed: ' + str(len(message)))
    logger.info(f'Total message tokens inserted: ' + str(total_tokens_inserted))
