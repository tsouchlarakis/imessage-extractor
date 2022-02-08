"""Main module for the streamlit app"""
# https://chatvisualizer.com/

import logging
import sqlite3
import streamlit as st
from send2trash import send2trash
import typing
from imessage_extractor.src.app.data.extract import iMessageDataExtract
from imessage_extractor.src.helpers.verbosity import logger_setup
from os.path import join, expanduser, dirname, isfile, isdir
from os import mkdir
from pages import about, home, my_stats
from pages.pick_a_contact import pick_a_contact


refresh_data = False  # Used for debugging and testing
tmp_imessage_visualizer_dpath = expanduser('~/.imessage_visualizer')


PAGES = {
    'Home': home,
    'My Stats': my_stats,
    'Pick a Contact': pick_a_contact,
    # 'Resources': resources,
    # 'Gallery': gallery.index,
    # 'Vision': vision,
    'About': about,
}

logger = logger_setup(name='imessage-visualizer', level=logging.INFO)
logger.propagate = False


class MultiApp(object):
    """
    Framework for combining multiple streamlit applications.
    """
    def __init__(self, chatdb_con: sqlite3.Connection, logger: logging.Logger):
        self.apps = []
        self.logger = logger

        if not isdir(tmp_imessage_visualizer_dpath):
            mkdir(tmp_imessage_visualizer_dpath)

        loading_text = 'Loading iMessage data...'
        with st.spinner(loading_text):
            if refresh_data:
                self.data = iMessageDataExtract(chatdb_con, logger)
                self.data.extract_data()
                self.data.save_data_extract(tmp_imessage_visualizer_dpath)

            else:
                if isdir(tmp_imessage_visualizer_dpath):
                    self.data = iMessageDataExtract(chatdb_con, logger)
                    self.data.load_data_extract(tmp_imessage_visualizer_dpath)
                else:
                    self.data = iMessageDataExtract(chatdb_con, logger)
                    self.data.extract_data()
                    self.data.save_data_extract(tmp_imessage_visualizer_dpath)


    def add_app(self, title: str, write_func: typing.Callable):
        """
        write_func: the python function to render this app.
        title: title of the app. Appears in the dropdown in the sidebar.
        """
        self.apps.append({
            'title': title,
            'function': write_func,
        })

    def run(self):
        """
        Run the app.
        """
        # Set the name of the page that the app should open to when loaded
        default_page_title = 'My Stats'

        app = st.sidebar.radio(
            label='',
            options=self.apps,
            index=[x['title'] for x in self.apps].index(default_page_title),
            format_func=lambda app: app['title']
        )

        app['function'](data=self.data, logger=self.logger)


def activate_stylesheet(fpath: str) -> None:
    """
    Read a local CSS stylesheet and set it using st.markdown().
    """
    with open(fpath, 'r') as f:
        stylesheet = f.read()

    st.markdown(f'<style>{stylesheet}</style>', unsafe_allow_html=True)


def main():
    """
    Top-level function of the app.
    """
    activate_stylesheet(join(dirname(__file__), 'stylesheet.css'))

    imessage_extractor_chatdb_path = expanduser('~/GDrive/Hobbies/Code/git-doni/imessage-extractor/database/imessage_extractor.db')
    chatdb_con = sqlite3.connect(imessage_extractor_chatdb_path)

    st.sidebar.title('Navigation')

    app = MultiApp(logger=logger, chatdb_con=chatdb_con)

    for page_name, page_module in PAGES.items():
        app.add_app(page_name, getattr(page_module, 'write'))

    app.run()

    st.sidebar.title('Contribute')
    st.sidebar.info("""
    This is an open-source project and you're very welcome to contribute any and all ideas
    for improvements, bug fixes or new features as
    [pull requests](https://github.com/tsouchlarakis/imessage-extractor/pulls) or
    [issues](https://github.com/tsouchlarakis/imessage-extractor/issues) to the
    [source code](https://github.com/tsouchlarakis/imessage-extractor). For more details,
    see the repository's [README](https://github.com/tsouchlarakis/imessage-extractor/blob/main/README.rst).
    """)

    st.sidebar.title('About')
    st.sidebar.info("""
    This app is developed by Andoni Sooklaris. You can learn more about me at
    [andonisooklaris.com](https://www.andonisooklaris.com).
    """)

    if refresh_data:
        send2trash(tmp_imessage_visualizer_dpath)


if __name__ == '__main__':
    main()
