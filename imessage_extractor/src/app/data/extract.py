import emoji
import json
from send2trash import send2trash
import logging
import _pickle as cPickle
import pandas as pd
import streamlit as st
import string
import sqlite3
from os.path import join, expanduser, dirname
from imessage_extractor.src.helpers.verbosity import code, path
from imessage_extractor.src.app.helpers import extract_exists, get_extract_fpath, get_db_fpath
import subprocess


@st.cache(show_spinner=False, allow_output_mutation=True)
class iMessageDataExtract(object):
    """
    Store all dataframe extract objects accessed in the GUI.
    """
    def __init__(self, chatdb_fpath: str, logger: logging.Logger) -> None:
        self.logger = logger
        self.chatdb_fpath = chatdb_fpath

    def extract_data(self) -> None:
        """
        Perform iMessage data extract.
        """
        #
        # Call imessage-extractor
        #

        if hasattr(st.session_state, 'chatdb_fpath'):
            system_chatdb_fpath = st.session_state.chatdb_fpath
        else:
            system_chatdb_fpath = expanduser('~/Library/Messages/chat.db')

        self.logger.info(f'Running {code("imessage-extractor")} workflow', arrow='black')
        subprocess.call(['imessage-extractor', 'go', '--chatdb-path', system_chatdb_fpath, '--output-db-path', get_db_fpath()])

        #
        # Raw tables
        #

        with open(join(dirname(__file__), 'manifest.json'), 'r') as f:
            manifest = json.load(f)

        self.logger.info(f'Read {path("manifest.json")}, extracting {len(manifest)} total datasets', arrow='black')

        for dataset_name, dataset_metadata in manifest.items():
            self.logger.debug(f'Reading dataset {code(dataset_name)}...', arrow='black')

            with sqlite3.connect(self.chatdb_fpath) as con:
                dataset = pd.read_sql(f'select * from {dataset_name}', con)

            if 'dt' in dataset.columns:
                dataset['dt'] = pd.to_datetime(dataset['dt'])

            if 'ts' in dataset.columns:
                dataset['ts'] = dataset['ts'].apply(lambda x: pd.to_datetime(x).tz_localize(None))

            # if dataset_metadata['index'] is not None:
            #     dataset = dataset.set_index(dataset_metadata['index'])

            setattr(self, dataset_name, dataset)
            self.logger.info(f'Read {code(dataset_name)}, shape: {dataset.shape}', arrow='black')

        #
        # Lists
        #

        self.lst_contact_names_all = sorted([x for x in self.message_user['contact_name'].combine_first(self.message_user['chat_identifier']).unique() if isinstance(x, str)])
        self.lst_contact_names_no_group_chats = sorted(
            [x for x in self.message_user.loc[self.message_user['is_group_chat'] == 0]['contact_name'].unique() if isinstance(x, str)]
        )
        self.lst_punctuation_chars = list(string.punctuation + '’‘“”``')
        self.lst_contractions_w_apostrophe = [
            "i'm", "i'd", "i've", "i'll", "'s",
        ]
        self.lst_contractions_wo_apostrophe = [x.replace("'", '') for x in self.lst_contractions_w_apostrophe]

        self.lst_emojis = list(emoji.UNICODE_EMOJI['en'].keys())

        self.map_tapback_type = {
            'emote_dislike': 'Dislike',
            'emote_emphasis': 'Emphasis',
            'emote_laugh': 'Laugh',
            'emote_like': 'Like',
            'emote_love': 'Love',
            'emote_question': 'Question',
            'emote_remove_dislike': 'Remove Dislike',
            'emote_remove_emphasis': 'Remove Emphasis',
            'emote_remove_heart': 'Remove Heart',
            'emote_remove_laugh': 'Remove Laugh',
            'emote_remove_like': 'Remove Like',
            'emote_remove_question': 'Remove Question',
        }
        self.lst_tapbacks = list(self.map_tapback_type.keys())

        self.logger.info('Defined lists', arrow='black')

    def get_dataset_names(self) -> list:
        """
        Get datasets pulled in the .extract_data() method.
        """
        return [x for x in dir(self)  if not x.startswith('__') and not callable(getattr(self, x)) and x not in ['logger']]

    def save_data_extract(self, dpath: str) -> None:
        """
        Save iMessage Data Extract to local using pickle.
        """
        dataset_names_to_save = self.get_dataset_names()

        # for dataset_name in dataset_names_to_save:
        #     fpath = join(dpath, f'{dataset_name}.pkl')
        #     with open(fpath, 'wb') as f:
        #         cPickle.dump(getattr(self, dataset_name), f)

        if extract_exists():
            send2trash(get_extract_fpath())

        pickle_dict = {}
        for dataset_name in dataset_names_to_save:
            pickle_dict[dataset_name] = getattr(self, dataset_name)

        with open(get_extract_fpath(), 'wb') as f:
            cPickle.dump(pickle_dict, f)

        self.logger.info(f'Saved iMessage data extract to {path(dpath)}', arrow='black')

    def load_data_extract(self, dpath: str) -> None:
        """
        Load iMessage Data Extract from local using pickle.
        """
        if extract_exists():
            with open(get_extract_fpath(), 'rb') as f:
                pickle_dict = cPickle.load(f)

            for dataset_name, dataset in pickle_dict.items():
                setattr(self, dataset_name, dataset)

            self.logger.info(f'Loaded iMessage data extract from {path(dpath)}', arrow='black')

        # dataset_fpaths_to_load = [splitext(x)[0] for x in listdir(dpath)]
        # if len(dataset_fpaths_to_load):
        #     for dataset_name in dataset_fpaths_to_load:
        #         fpath = join(dpath, f'{dataset_name}.pkl')
        #         if isfile(fpath):
        #             with open(fpath, 'rb') as f:
        #                 setattr(self, dataset_name, cPickle.load(f))
        #         else:
        #             raise FileNotFoundError(f'Attempting to load {path(fpath)} since {code("refresh_data")} is set to False, but the file does not exist')

        #     self.logger.info(f'Loaded iMessage data extract from {path(dpath)}', arrow='black')

        # else:
        #     # Just extract data as usual
        #     self.logger.info(f'{code("refresh_data")} is set to False, but no data extract files were found in {path(dpath)}, extracting data as normal', arrow='black')
        #     self.extract_data()
        #     self.save_data_extract(dpath)
