import datetime
import humanize


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