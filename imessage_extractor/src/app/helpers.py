import datetime


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


def wrap_tag(tag: str, text: str, cls: str=None) -> str:
    if cls:
        return f'<{tag} class="{cls}">{str(text)}</{tag}>'
    else:
        return f'<{tag}>{str(text)}</{tag}>'


def span(text: str, cls: str=None) -> str:
    return wrap_tag('span', str(text), cls=cls)


def large_text(text: str) -> str:
    return span(str(text), cls='large-text')


def large_text_green(text: str) -> str:
    return span(str(text), cls='large-text-green')


def medium_text(text: str) -> str:
    return span(str(text), cls='medium-text')


def medium_text_green(text: str) -> str:
    return span(str(text), cls='medium-text-green')