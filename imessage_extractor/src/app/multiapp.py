"""
Frameworks for running multiple Streamlit applications as a single app.

Source: https://github.com/upraneelnihar/streamlit-multiapps/blob/master/multiapp.py
"""
import streamlit as st
import typing
import logging
import pandas as pd
from pydoni import Postgres


@st.cache(show_spinner=True)
class iMessageDataExtract(object):
    """
    Store all dataframe extract objects accessed in the GUI.
    """
    def __init__(self, logger: logging.Logger) -> None:
        pg = Postgres()
        logger.info('Fetching iMessageDataExtract')

        #
        # Raw tables
        #
        sample = False

        if sample:
            sample_proportion = .01
            logger.info(f'Querying SQL tables with {sample_proportion * 100}% sample')
            self.message_vw = pg.read_sql(f'select * from imessage_extractor.message_vw where random() < {sample_proportion}')
            self.message_vw_text = pg.read_sql(f'select * from imessage_extractor.message_vw_text where random() < {sample_proportion}')
        else:
            logger.info('Querying full SQL tables')
            self.message_vw = pg.read_table('imessage_extractor', 'message_vw')
            self.message_vw_text = pg.read_table('imessage_extractor', 'message_vw_text')

        logger.info('Done querying SQL tables')

        #
        # Lists
        #

        self.lst_contact_names_all = sorted([x for x in self.message_vw['contact_name'].unique() if isinstance(x, str)])
        self.lst_contact_names_no_group_chats = sorted(
            [x for x in self.message_vw.loc[~self.message_vw['is_group_chat']]['contact_name'].unique() if isinstance(x, str)]
        )

        #
        # Daily summaries
        #

        logger.info('Aggregating data into daily summaries')

        daily_aggregations = lambda x: pd.Series(dict(
            n_messages=x.message_id.nunique(),
            n_text_messages=(x.is_text == True).sum(),
            n_messages_group_chat=(x.is_group_chat == True).sum(),
            n_text_messages_group_chat=((x.is_text == True) & (x.is_group_chat == True)).sum(),
            contacts_messaged=x.contact_name.nunique(),
            n_imessage=(x.service == 'iMessage').sum(),
            n_sms=(x.service == 'SMS').sum(),
            n_url=(x.is_url == True).sum(),
            n_thread_origin=(x.is_thread_origin == True).sum(),
            n_threaded_reply=(x.is_threaded_reply == True).sum(),
            n_attachment=(x.has_attachment == True).sum(),
        ))

        self.daily_summary = self.message_vw.groupby('dt').apply(daily_aggregations)
        self.daily_summary_contact = self.message_vw.groupby(['dt', 'contact_name']).apply(daily_aggregations)
        self.daily_summary_from_who = self.message_vw.groupby(['dt', 'is_from_me']).apply(daily_aggregations)
        self.daily_summary_contact_from_who = self.message_vw.groupby(['dt', 'contact_name', 'is_from_me']).apply(daily_aggregations)

        logger.info('Computed daily summaries')

        logger.info('Completed iMessageDataExtract')


class MultiApp(object):
    """
    Framework for combining multiple streamlit applications.
    """
    def __init__(self, logger: logging.Logger):
        self.apps = []
        self.logger = logger
        self.data = iMessageDataExtract(logger)

        self.logger.info('Initialized MultiApp')

    def add_app(self, title: str, write_func: typing.Callable):
        """
        Adds a new application.

        Parameters
        ----------
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
        app = st.sidebar.radio('', self.apps, format_func=lambda app: app['title'])
        app['function'](data=self.data, logger=self.logger)
