import click
import logging
import re
import threading
from os.path import dirname, join, expanduser


class ExtendedLogger(logging.Logger):
    """
    Extend the logging.Logger class.
    """
    def __init__(self, name, level=logging.NOTSET) -> None:
        self._count = 0
        self._countLock = threading.Lock()
        return super(ExtendedLogger, self).__init__(name, level)

    def _build_message(self, msg: str, arrow: str=None, indent: int=0, bold: bool=False) -> str:
        """
        Apply format parameters to a raw string.
        """
        msg = re.sub(r'\s+', ' ', msg.strip())

        if bold:
            msg = click.style(msg, bold=True)

        arrow_str = click.style('> ', fg=arrow, bold=True) if arrow is not None else ''
        indent_str = '  ' * indent

        msg = str(msg)
        return f'{indent_str} {arrow_str}{msg}'

    def debug(self, msg: str, *args, **kwargs):
        """
        Override the logging.Logger.debug() method.
        """
        formatted_msg = self._build_message(msg, *args, **kwargs)
        return super(ExtendedLogger, self).debug(formatted_msg)

    def info(self, msg: str, *args, **kwargs):
        """
        Override the logging.Logger.info() method.
        """
        formatted_msg = self._build_message(msg, *args, **kwargs)
        return super(ExtendedLogger, self).info(formatted_msg)

    def warning(self, msg: str, *args, **kwargs):
        """
        Override the logging.Logger.warning() method.
        """
        formatted_msg = self._build_message(msg, *args, **kwargs)
        return super(ExtendedLogger, self).warning(formatted_msg)

    def error(self, msg: str, *args, **kwargs):
        """
        Override the logging.Logger.error() method.
        """
        formatted_msg = self._build_message(msg, *args, **kwargs)
        return super(ExtendedLogger, self).error(formatted_msg)

    def critical(self, msg: str, *args, **kwargs):
        """
        Override the logging.Logger.critical() method.
        """
        formatted_msg = self._build_message(msg, *args, **kwargs)
        return super(ExtendedLogger, self).critical(formatted_msg)


def logger_setup(name: str=__name__, level: int=logging.DEBUG) -> logging.Logger:
    """
    Standardize logger setup.
    """
    logging.setLoggerClass(ExtendedLogger)
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s :%(message)s')

    if logger.hasHandlers():
        logger.handlers.clear()

    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.setLevel(level)
    return logger


def print_startup_message(logger: logging.Logger) -> None:
    """
    Print startup message to console.
    """
    tab = '    '  # This is used in `msg_fmt` format string

    with open(join(dirname(__file__), 'startup_message.txt'), 'r') as f:
        msg = f.read()
        msg_fmt = eval("f'''{}'''".format(msg))
        msg_lst = msg_fmt.split('\n')
        for line in msg_lst:
            logger.info(line)


def bold(msg: str) -> str:
    """
    Return a string wrapped in bold.
    """
    return click.style(str(msg), bold=True)


def path(msg: str, home_tilde: bool=True) -> str:
    """
    Return a string formatted as a colored path string.
    """
    if home_tilde:
        if msg.startswith(expanduser('~')):
            msg = msg.replace(expanduser('~'), '~')

    return click.style(str(msg), fg='blue')


def code(msg: str) -> str:
    """
    Return a string formatted as a colored code string.
    """
    return click.style(str(msg), fg='black')
