"""
Frameworks for running multiple Streamlit applications as a single app.

Source: https://github.com/upraneelnihar/streamlit-multiapps/blob/master/multiapp.py
"""
import streamlit as st
import typing
import logging
import json
from os import mkdir
from os.path import join, expanduser, isdir, basename
from shutil import rmtree
from sql_query_tools import Postgres
from imessage_extractor.src.helpers.verbosity import path
from imessage_extractor.src.helpers.utils import ensurelist
from imessage_extractor.src.app.data.extract import iMessageDataExtract


@st.cache(show_spinner=False)
def save_data_to_local(tmp_env_dpath: str, logger: logging.Logger):
    """
    Query all necessary tables/views and save the data as CSV files to a local temp directory.
    """
    logger.info('Querying data and saving to local...')
    logger.info(f'=> target folder {path(tmp_env_dpath)}')
    pg = Postgres()

    # Maintain a dictionary of all tables written to local
    manifest = {}


    def query_and_save_dataset(pg_schema: str,
                               dataset_name: str,
                               index: list,
                               tmp_env_dpath: str,
                               logger: logging.Logger):
        """
        Read the full table/view and save to local CSV.
        """
        if index is not None:
            index = ensurelist(index)
            write_index = True
        else:
            write_index = False

        df = pg.read_table(pg_schema, dataset_name)
        logger.info(f'=> {dataset_name} queried')

        fpath = join(tmp_env_dpath, f'{dataset_name}.csv')

        if write_index:
            df.set_index(index).to_csv(fpath, index=True)
        else:
            df.to_csv(fpath, index=False)

        logger.info(f'=> {dataset_name} saved {path(basename(fpath))}')

        return fpath


    index = ['message_id']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='message_vw',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['message_vw'] = dict(fpath=fpath, index=index)

    index = ['message_id']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='message_vw_text',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['message_vw_text'] = dict(fpath=fpath, index=index)

    index = ['message_id']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='message_tokens_unnest',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['message_tokens_unnest'] = dict(fpath=fpath, index=index)

    index = ['dt', 'contact_name', 'is_from_me']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='daily_summary_contact_from_who',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['daily_summary_contact_from_who'] = dict(fpath=fpath, index=index)

    index = ['dt', 'contact_name']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='daily_summary_contact',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['daily_summary_contact'] = dict(fpath=fpath, index=index)

    index = ['dt', 'is_from_me']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='daily_summary_from_who',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['daily_summary_from_who'] = dict(fpath=fpath, index=index)

    index = ['dt']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='daily_summary',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['daily_summary'] = dict(fpath=fpath, index=index)

    index = ['contact_name', 'is_from_me', 'token', 'length']
    fpath = query_and_save_dataset(pg_schema='imessage_extractor',
                                   dataset_name='contact_token_usage_from_who',
                                   index=index,
                                   tmp_env_dpath=tmp_env_dpath,
                                   logger=logger)
    manifest['contact_token_usage_from_who'] = dict(fpath=fpath, index=index)

    manifest_fpath = join(tmp_env_dpath, 'manifest.json')
    with open(manifest_fpath, 'w') as f:
        if len(manifest) == 0:
            raise ValueError('manifest variable is of length 0!')

        json.dump(manifest, f)

    logger.info(f'=> wrote manifest {path(manifest_fpath)}')

    logger.info('=> done')


class MultiApp(object):
    """
    Framework for combining multiple streamlit applications.
    """
    def __init__(self, logger: logging.Logger):
        self.apps = []
        self.logger = logger
        self.tmp_env_dpath = join(expanduser('~'), 'Desktop', '.imessage-extractor-app')

        # Extract and save data to temporary folder
        if isdir(self.tmp_env_dpath):
            rmtree(self.tmp_env_dpath)

        if not isdir(self.tmp_env_dpath):
            mkdir(self.tmp_env_dpath)

        with st.spinner('Loading iMessage data...'):
            save_data_to_local(self.tmp_env_dpath, logger)

        self.data = iMessageDataExtract(self.tmp_env_dpath, logger)
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
        # Set the name of the page that the app should open to when loaded
        default_page_title = 'Pick a Contact'
        app = st.sidebar.radio(
            label='',
            options=self.apps,
            index=[x['title'] for x in self.apps].index(default_page_title),
            format_func=lambda app: app['title']
        )
        app['function'](data=self.data, logger=self.logger)
