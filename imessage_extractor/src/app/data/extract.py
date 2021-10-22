import logging
import pandas as pd
import string
import streamlit as st
import json
from os.path import join


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

        self.lst_contact_names_all = sorted([x for x in self.message_vw['contact_name'].unique() if isinstance(x, str)])
        self.lst_contact_names_no_group_chats = sorted(
            [x for x in self.message_vw.loc[~self.message_vw['is_group_chat']]['contact_name'].unique() if isinstance(x, str)]
        )
        self.lst_punctuation_chars = list(string.punctuation + '’‘“”``')
        self.lst_contractions_w_apostrophe = [
            "i'm", "i'd", "i've", "i'll", "'s",
        ]
        self.contractions_wo_apostrophe = [x.replace("'", '') for x in self.lst_contractions_w_apostrophe]

        logger.info('=> lists computed')

        logger.info('=> done')
