"""
Frameworks for running multiple Streamlit applications as a single app.

Source: https://github.com/upraneelnihar/streamlit-multiapps/blob/master/multiapp.py
"""
import streamlit as st
import typing
from pydoni import Postgres


@st.cache(show_spinner=True)
class iMessageDataExtract(object):
    """
    Store all dataframe extract objects accessed in the GUI.
    """
    def __init__(self) -> None:
        pg = Postgres()

        #
        # Raw tables
        #

        self.message_vw = pg.read_table('imessage_extractor', 'message_vw')
        self.message_vw_text = pg.read_table('imessage_extractor', 'message_vw_text')

        # self.message_vw = pg.read_sql('select * from imessage_extractor.message_vw limit 1000')
        # self.message_vw_text = pg.read_sql('select * from imessage_extractor.message_vw_text limit 1000')

        #
        # Lists
        #

        self.lst_all_contact_names = sorted([x for x in self.message_vw['contact_name'].unique() if isinstance(x, str)])

        #
        # Aggregated statistics
        #

        self.message_count_daily = (
            self.message_vw
            .groupby('dt')
            .agg({
                'message_id': 'count',
                'contact_name': 'nunique',
            })
        )

        #
        # Aggregated by contact
        #

        self.contact_active_texting_days = (
            self.message_vw
            .groupby('contact_name')
            .agg({'dt': 'nunique'})
            .rename(columns={'dt': 'active_texting_days'})
            .reset_index()
            .sort_values('active_texting_days', ascending=False)
            .set_index('contact_name')
        )


data = iMessageDataExtract()


class MultiApp:
    """
    Framework for combining multiple streamlit applications.
    """
    def __init__(self):
        self.apps = []

    def add_app(self, title: str, write_func: typing.Callable):
        """Adds a new application.
        Parameters
        ----------
        write_func: the python function to render this app.
        title: title of the app. Appears in the dropdown in the sidebar.
        """
        self.apps.append({
            'title': title,
            'function': write_func
        })

    def run(self):
        app = st.sidebar.radio('', self.apps, format_func=lambda app: app['title'])
        app['function'](data=data)