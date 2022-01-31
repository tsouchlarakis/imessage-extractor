"""Main module for the streamlit app"""
# https://chatvisualizer.com/

import streamlit as st
import logging
import multiapp
from imessage_extractor.src.helpers.verbosity import logger_setup
from os.path import join, dirname
from pages import home
from pages import about
from pages import my_stats
from pages.pick_a_contact import pick_a_contact


PAGES = {
    'Home': home,
    'My Stats': my_stats,
    'Pick a Contact': pick_a_contact,
    # 'Resources': resources,
    # 'Gallery': gallery.index,
    # 'Vision': vision,
    'About': about,
}

logger = logger_setup(name='imessage-visualizer', level=logging.DEBUG)
logger.propagate = False


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

    st.sidebar.title('Navigation')

    app = multiapp.MultiApp(logger=logger)

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


if __name__ == "__main__":
    main()