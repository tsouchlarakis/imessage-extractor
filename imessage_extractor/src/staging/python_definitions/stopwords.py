import nltk
if nltk.download('stopwords', quiet=True) == False:
    import subprocess
    subprocess.call('/Applications/Python\ 3.9/Install\ Certificates.command', shell=True)

from nltk.corpus import stopwords

import logging
import pandas as pd
from imessage_extractor.src.chatdb.chatdb import ChatDb
from imessage_extractor.src.helpers.verbosity import bold, code
from imessage_extractor.src.staging.common import columns_match_expectation


def refresh_stopwords(chatdb: 'ChatDb',
                      table_name: str,
                      columnspec: dict,
                      logger: logging.Logger
                      ) -> None:
    """
    Refresh table emoji_text_map.
    """
    logger.debug(f'Refreshing table "{bold(table_name)}"', arrow='yellow')

    stopwords_df = pd.DataFrame(stopwords.words('english'))
    stopwords_df.rename(columns={0: 'stopword'}, inplace=True)

    columns_match_expectation(stopwords_df, table_name, columnspec)
    stopwords_df.to_sql(name=table_name,
                        con=chatdb.sqlite_con,
                        schema='main',
                        index=False,
                        if_exists='replace')

    logger.info(f'Built table {code(table_name)}', arrow='black')
