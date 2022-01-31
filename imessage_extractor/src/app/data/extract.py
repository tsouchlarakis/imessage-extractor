import emoji
import json
import logging
import pandas as pd
import streamlit as st
import string
from os.path import join
from imessage_extractor.src.helpers.verbosity import code


@st.cache(show_spinner=False, allow_output_mutation=True)
class iMessageDataExtract(object):
    """
    Store all dataframe extract objects accessed in the GUI.
    """
    def __init__(self, tmp_env_fpath: str, logger: logging.Logger) -> None:
        logger.info('Initializing iMessageDataExtract...')

        #
        # Raw tables
        #

        with open(join(tmp_env_fpath, 'manifest.json'), 'r') as f:
            manifest = json.load(f)

        logger.info('=> read manifest')

        for dataset_name, dataset_metadata in manifest.items():
            logger.debug(f'Reading dataset {code(dataset_name)}...')
            dataset = pd.read_csv(dataset_metadata['fpath'])

            if 'dt' in dataset.columns:
                dataset['dt'] = pd.to_datetime(dataset['dt'])

            if 'ts' in dataset.columns:
                dataset['ts'] = dataset['ts'].apply(lambda x: pd.to_datetime(x).tz_localize(None))

            if dataset_metadata['index'] is not None:
                dataset = dataset.set_index(dataset_metadata['index'])

            setattr(self, dataset_name, dataset)
            logger.info(f'=> read {dataset_name}')

        #
        # Lists
        #

        self.lst_contact_names_all = sorted([x for x in self.message_user['contact_name'].unique() if isinstance(x, str)])
        self.lst_contact_names_no_group_chats = sorted(
            [x for x in self.message_user.loc[self.message_user['is_group_chat'] == 0]['contact_name'].unique() if isinstance(x, str)]
        )
        self.lst_punctuation_chars = list(string.punctuation + '’‘“”``')
        self.lst_contractions_w_apostrophe = [
            "i'm", "i'd", "i've", "i'll", "'s",
        ]
        self.contractions_wo_apostrophe = [x.replace("'", '') for x in self.lst_contractions_w_apostrophe]

        self.emoji_lst = list(emoji.UNICODE_EMOJI['en'].keys())

        self.tapback_type_map = {
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
        self.tapback_lst = list(self.tapback_type_map.keys())

        logger.info('=> lists computed')

        logger.info('=> done')
