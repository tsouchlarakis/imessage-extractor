import pdb
import click  # Required by `print_startup_message()`
import logging
import threading
import re
from collections import OrderedDict
from pyfiglet import Figlet
from os.path import dirname, join


class ColorTag(object):
    """
    Object to be passed into Verbose class methods to append colorized tags to the
    start of a message printed to console.
    """
    def __init__(self) -> None:
        # Pairs of tag_name: tag_color. Color to be passed into `click.style()`
        self.color_map = {
            'dry-run': 'magenta',
            'full-refresh': 'red',
            'step-name': 'blue',

            # Transform step names
            'contact-aggregated-stats': 'blue',
            'emoji-text-map': 'blue',
            'message-emoji-map': 'blue',
            'message-tokens': 'blue',
            'tokens': 'blue',
        }

        # Paris of tag_name: tag_color
        self.stack = OrderedDict()

    def add(self, tag_name) -> None:
        """Add a tag to stack."""
        self._assert_valid_tag(tag_name)
        if not self.has(tag_name):
            self.stack[tag_name] = self.color_map[tag_name]

    def has(self, tag_name) -> bool:
        """Indicate logically whether a particular tag name is in stack."""
        return tag_name in self.stack.keys()

    def remove(self, tag_name) -> None:
        """Attempt to remove a tag from stack."""
        assert tag_name in self.stack.keys(), f"Tag '{tag_name}' not in stack"
        self.stack = OrderedDict({k: v for k, v in self.stack.items() if k != tag_name})

    def reset(self) -> None:
        """Clear all tags from stack."""
        self.stack = OrderedDict()

    def _assert_valid_tag(self, tag_name) -> bool:
        """
        Check whether a tag is present in color map. Otherwise, it's an invalid tag unless
        added to the color map.
        """
        msg = f"Tag '{tag_name}' not in color map. Acceptable tags are: {', '.join(list(self.color_map.keys()))}"
        assert tag_name in self.color_map.keys(), msg


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

        arrow_str = click.style('==> ', fg=arrow, bold=True) if arrow is not None else ''
        indent_str = '  ' * indent

        msg = str(msg)
        return f'{indent_str} {arrow_str}{msg}'

    # def debug(self, msg: str, *args, **kwargs):
    #     """
    #     Override the logging.Logger.debug() method.
    #     """
    #     import pdb; pdb.set_trace()
    #     formatted_msg = self._build_message(msg, *args, **kwargs)
    #     return super(ExtendedLogger, self).debug(formatted_msg)

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


def logger_setup(name: str=__name__, level: int=logging.DEBUG, equal_width: bool=False):
    """
    Standardize logger setup across pydoni package.
    """
    logging.setLoggerClass(ExtendedLogger)
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(level)

    return logger


def print_startup_message(logger: logging.Logger) -> None:
    """
    Print startup message to console.
    """
    tab = '    '  # This is used in `msg_fmt` format string
    fig = Figlet(font='slant')

    header_color = 'red'
    header = fig.renderText('iMessage Extractor')
    for s in str(header).split('\n'):
        logger.info(click.style(s, fg=header_color))

    logger.info('')

    with open(join(dirname(__file__), 'startup_message.txt'), 'r') as f:
        msg = f.read()
        msg_fmt = eval("f'''{}'''".format(msg))
        msg_lst = msg_fmt.split('\n')
        for line in msg_lst:
            logger.info(line)
            # time.sleep(.015)

        # time.sleep(1.5)

def bold(msg: str) -> str:
    """
    Return a string wrapped in bold.
    """
    return click.style(msg, bold=True)


def path(msg: str) -> str:
    """
    Return a string formatted as a colored path string.
    """
    return click.style(msg, fg='blue')


def code(msg: str) -> str:
    """
    Return a string formatted as a colored code string.
    """
    return click.style(msg, fg='black')