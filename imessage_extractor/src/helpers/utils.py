import os
import pathlib
import re
import typing


def fmt_seconds(time_in_sec: int, units: str='auto', round_digits: int=4) -> dict:
    """
    Format time in seconds to a custom string. `units` parameter can be
    one of 'auto', 'seconds', 'minutes', 'hours' or 'days'.
    """
    if units == 'auto':
        if time_in_sec < 60:
            time_diff = round(time_in_sec, round_digits)
            time_measure = 'seconds'
        elif time_in_sec >= 60 and time_in_sec < 3600:
            time_diff = round(time_in_sec/60, round_digits)
            time_measure = 'minutes'
        elif time_in_sec >= 3600 and time_in_sec < 86400:
            time_diff = round(time_in_sec/3600, round_digits)
            time_measure = 'hours'
        else:
            time_diff = round(time_in_sec/86400, round_digits)
            time_measure = 'days'

    elif units in ['seconds', 'minutes', 'hours', 'days']:
        time_measure = units
        if units == 'seconds':
            time_diff = round(time_in_sec, round_digits)
        elif units == 'minutes':
            time_diff = round(time_in_sec/60, round_digits)
        elif units == 'hours':
            time_diff = round(time_in_sec/3600, round_digits)
        else:
            # Days
            time_diff = round(time_in_sec/86400, round_digits)

    return dict(zip(['units', 'value'], [time_measure, time_diff]))


def human_filesize(nbytes: int) -> str:
    """
    Convert number of bytes to human-readable filesize string.
    Source: https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
    """
    base = 1

    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
        n = nbytes / base

        if n < 9.95 and unit != 'B':
            # Less than 10 then keep 1 decimal place
            value = '{:.1f} {}'.format(n, unit)
            return value

        if round(n) < 1000:
            # Less than 4 digits so use this
            value = f'{round(n)} {unit}'
            return value

        base *= 1024

    value = f'{round(n)} {unit}'

    return value


def strip_ws(string: str):
    """
    Strip whitespace off a string and replace all instances of >1 space with a single space.
    """
    return re.sub(r'\s+', ' ', string.strip())


def ensurelist(val: typing.Any) -> list:
    """
    Accept a string or list and ensure that it is formatted as a list. If `val` is not a list,
    return [val]. If `val` is already a list, return as is.
    """
    return [val] if not isinstance(val, list) else val


def listfiles(path: typing.Union[str, pathlib.Path]='.',
              ext=None,
              pattern=None,
              ignore_case=True,
              full_names=False,
              recursive=False,
              include_hidden=True) -> list:
    """
    List files in a given directory.

    path (str): absolute path to search for files in
    ext (str): optional file extension or list of extensions to filter resulting files by
    pattern (str): optional filter resulting files by matching regex pattern
    ignore_case (bool): do not consider case in when filtering for `pattern` parameter
    full_names (bool): return absolute filepaths
    recursive (bool): search recursively down the directory tree
    include_hidden (bool): include hidden files in resulting file list
    """
    owd = os.getcwd()
    os.chdir(path)

    if recursive:
        fpaths = []
        for root, dpaths, filenames in os.walk('.'):
            for f in filenames:
                fpaths.append(os.path.join(root, f).replace('./', ''))
    else:
        fpaths = [f for f in os.listdir() if os.path.isfile(f)]

    if not include_hidden:
        fpaths = [f for f in fpaths if not os.path.basename(f).startswith('.')]

    if pattern is not None:
        if ignore_case:
            fpaths = [f for f in fpaths if re.search(pattern, f, re.IGNORECASE)]
        else:
            fpaths = [f for f in fpaths if re.search(pattern, f)]

    if ext:
        ext = [x.lower() for x in ensurelist(ext)]
        ext = ['.' + x if not x.startswith('.') else x for x in ext]
        fpaths = [x for x in fpaths if os.path.splitext(x)[1].lower() in ext]

    if full_names:
        path_expand = os.getcwd() if path == '.' else path
        fpaths = [os.path.join(path_expand, f) for f in fpaths]

    os.chdir(owd)
    return fpaths


def duplicated(lst: list) -> list:
    """
    Return list of boolean values indicating whether each item in a list is a duplicate of
    a previous item in the list. Order matters!
    """
    dup_ind = []

    for i, item in enumerate(lst):
        tmplist = lst.copy()
        del tmplist[i]

        if item in tmplist:
            # Test if this is the first occurrence of this item in the list. If so, do not
            # count as duplicate, as the first item in a set of identical items should not
            # be counted as a duplicate

            first_idx = min(
                [i for i, x in enumerate(tmplist) if x == item])

            if i != first_idx:
                dup_ind.append(True)
            else:
                dup_ind.append(False)

        else:
            dup_ind.append(False)

    return dup_ind