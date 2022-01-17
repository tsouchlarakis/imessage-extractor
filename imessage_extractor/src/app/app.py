"""Main module for the streamlit app"""
# https://chatvisualizer.com/

import streamlit as st
import logging
import multiapp
from imessage_extractor.src.helpers.verbosity import logger_setup
from os.path import join, dirname
from pages import home
from pages import about
from pages.pick_a_contact import pick_a_contact
# import pages.resources
# import pages.vision


PAGES = {
    'Home': home,
    'Pick a Contact': pick_a_contact,
    # 'Resources': resources,
    # 'Gallery': gallery.index,
    # 'Vision': vision,
    'About': about,
}

logger = logger_setup(name='imessage-visualizer', level=logging.INFO)
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
    st.sidebar.info(
        'This an open source project and you are very welcome to **contribute** your awesome '
        'comments, questions, resources and apps as '
        '[issues](https://github.com/MarcSkovMadsen/awesome-streamlit/issues) of or '
        '[pull requests](https://github.com/MarcSkovMadsen/awesome-streamlit/pulls) '
        'to the [source code](https://github.com/MarcSkovMadsen/awesome-streamlit). '
    )
    st.sidebar.title('About')
    st.sidebar.info("""
    This app is maintained by Andoni Sooklaris. You can learn more about me at
    [andonisooklaris.com](https://www.andonisooklaris.com).
    """)



if __name__ == "__main__":
    main()