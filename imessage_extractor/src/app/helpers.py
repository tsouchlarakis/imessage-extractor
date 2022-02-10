import logging
import datetime
import humanize
from os.path import isfile, join, expanduser
from send2trash import send2trash


def to_date_str(dt: datetime.datetime) -> str:
    """
    Convert a date to a string in desired human-readable format.
    """
    if isinstance(dt, datetime.datetime) or isinstance(dt, datetime.date):
        return dt.strftime('%b %-d, %Y')
    elif isinstance(dt, str):
        try:
            return datetime.datetime.strptime(dt, '%Y-%m-%d').strftime('%b %-d, %Y')
        except ValueError:
            print(f'"{str(dt)}" (type: "{type(dt)}") is not a datetime or datetime-coercible object')
            return 'N/A'
    else:
        print(f'"{str(dt)}" (type: "{type(dt)}") is not a datetime or datetime-coercible object')
        return 'N/A'


def csstext(text: str, cls: str, span: bool=False, header: bool=False) -> str:
    """
    Custom build HTML text element.
    """
    if span:
        tag = 'span'
    elif header:
        tag = 'h1'
    else:
        tag = 'p'

    return f'<{tag} class="{cls}">{str(text)}</{tag}>'



def wrap_tag(tag: str, text: str, cls: str=None) -> str:
    if cls:
        return f'<{tag} class="{cls}">{str(text)}</{tag}>'
    else:
        return f'<{tag}>{str(text)}</{tag}>'


def htmlbold(text: str) -> str:
    return wrap_tag('b', text)


def intword(n: int) -> str:
    """
    Apply humanize.intword() and custom formatting thereafter.
    """
    str_map = dict(thousand='K', million='M', billion='B')
    word = humanize.intword(n)
    for k, v in str_map.items():
        word = word.replace(' ' + k, v)

    return word


def get_tmp_dpath() -> str:
    """
    Define the path for the temporary directory.
    """
    return join(expanduser('~'), '.imessage_visualizer')


def get_db_fpath() -> str:
    """
    Define the path for the copied chat.db.
    """
    return join(get_tmp_dpath(), 'imessage_extractor.db')


def get_extract_fpath() -> str:
    """
    Define the expected filepath.
    """
    return join(get_tmp_dpath(), 'imessage_visualizer_extract.pkl')


def extract_exists() -> bool:
    """
    Check whether an iMessage Visualizer extract already exists. It should always be
    at the same file location.
    """
    return isfile(get_extract_fpath())


def remove_extract(logger: logging.Logger) -> None:
    """
    Remove iMessage Visualizer extract if it exists.
    """
    if extract_exists():
        send2trash(get_extract_fpath())
        logger.info('Deleted old data extract .pkl', arrow='black')
