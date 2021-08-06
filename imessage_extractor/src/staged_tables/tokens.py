import logging
import emoji
import enchant
import langid
import nltk
import pydoni
import string
from nltk.corpus import stopwords
from tqdm import tqdm
from ..verbosity import bold


def is_punctuation(token):
    """
    Determine whether a token is punctuation.
    """
    return token in string.punctuation + '’‘“”``'


def detect_token_language(pg, token):
    """
    Given a token string, determine the language of a token either as a natural, human
    language, as an emoji character, or a punctuation mark.
    """
    if is_punctuation(token):
        return 'punct'

    emoji_lst = [k for k, v in emoji.UNICODE_EMOJI.items()]
    if token in emoji_lst:
        return 'emoji'

    d = enchant.Dict('en_US')
    if d.check(token):
        # Token is English word
        return 'en'


    # Attempt to classify token using `langid`
    lang_code = langid.classify(token)[0]
    if lang_code in ['nl', 'de', 'el', 'fr']:
        d = enchant.Dict(lang_code)
        if d.check(token):
            # Token is in the language's dictionary
            return lang_code

        # Token is not in language's dictionary, but could still be a token
        # in the language parsed by `langid`
        if lang_code == 'el':
            uniquely_greek_chars = [
                'α', 'β', 'Γ', 'γ', 'Δ', 'δ', 'ε', 'ζ', 'η', 'Θ', 'θ', 'ι', 'κ', 'Λ', 'λ', 'μ',
                'ν', 'Ξ', 'ξ', 'Ο', 'ο', 'Π', 'π', 'Ρ', 'ρ', 'Σ', 'σ', 'ς', 'Τ', 'τ', 'Υ', 'υ',
                'Φ', 'φ', 'Χ', 'χ', 'Ψ', 'ψ', 'Ω', 'ω',
            ]
            if any([x in token for x in uniquely_greek_chars]):
                return lang_code

    # Token unable to be classified, assign English as default
    return 'en'


def refresh_tokens(pg: pydoni.Postgres,
                   pg_schema: str,
                   table_name: str,
                   limit: int,
                   logger: logging.Logger):
    """
    Map each unique token to descriptor columns (stem, lemma, length, language, etc.).
    """
    message_tokens_table_name = 'message_tokens'

    trigger_name = pg_schema + '_' + table_name + '_rmod'
    if not pg.trigger_exists(pg_schema, trigger_name):
        pg.execute(f"""
        create or replace trigger {trigger_name}
        before update on "{pg_schema}"."{table_name}"
        for each row execute procedure set_rmod()""")

    logger.info(f'Created trigger "{bold(trigger_name)}"')

    new_tokens = pg.read_sql(f"""
    select distinct lower(t1.token) as "token"
    from {pg_schema}.{message_tokens_table_name} t1
    left join {pg_schema}.{table_name} t2
           on lower(t1.token) = lower(t2.token)
    where t2.token is null
    """, simplify=False)
    logger.info(f'Total new unique tokens: {len(new_tokens)}')

    if len(new_tokens) > 0:
        if isinstance(limit, int):
            new_tokens = new_tokens.head(limit)
            logger.info(f'Limited to {limit} new tokens')

        stops = stopwords.words('english')
        lemmatizer = nltk.stem.WordNetLemmatizer()
        stemmer = nltk.stem.PorterStemmer()

        def detect_token_language_(token):
            detect_token_language(pg, token)

        column_apply_function_map = {
            'length': len,
            'stem': stemmer.stem,
            'lemma': lemmatizer.lemmatize,
            'is_english_stopword': lambda tok: tok in stops,
            'is_punct': is_punctuation,
            'language': detect_token_language_,
        }

        for col, fun in column_apply_function_map.items():
            logger.info(f'Computing column: `{col}`')
            new_tokens[col] = new_tokens['token'].apply(fun)

        logger.info(f'Finished computing additional columns')

    logger.info(f'Appending tokens to {pg_schema}.{table_name}')
    n_new_tokens_str = len(new_tokens) if len(new_tokens) > 0 else 'no'

    new_tokens.to_sql(table_name, pg.dbcon, schema=pg_schema, index=False, if_exists='append')

    logger.info(f'Appended {n_new_tokens_str} new tokens to {pg_schema}.{table_name}')
