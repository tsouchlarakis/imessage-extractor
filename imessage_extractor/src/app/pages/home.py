import subprocess
from imessage_extractor.src.app.helpers import get_tmp_dpath, to_date_str, remove_extract, get_db_fpath
from send2trash import send2trash
from os.path import dirname, join, isdir, expanduser
from imessage_extractor.src.helpers.verbosity import code
from imessage_extractor.src.app.data.extract import iMessageDataExtract
import streamlit as st
import humanize
import logging


root_dir = dirname(dirname(dirname(dirname(dirname(__file__)))))
tmp_imessage_visualizer_dpath = get_tmp_dpath()


def write(data: 'iMessageDataExtract', logger: logging.Logger) -> None:
    """
    Write the home page.
    """
    logger.info(f'Writing page {code("Home")}', bold=True)

    st.image(join(root_dir, 'graphics', 'imessage_visualizer_logo.png'))
    latest_ts = data.message_user.ts.max()

    st.markdown(f"""
    Statistics on my iMessage history from
    **{to_date_str(data.message_user.dt.min())}**
    until
    **{to_date_str(data.message_user.dt.max())}.**
    Latest message sent or received
    **{humanize.naturaltime(latest_ts.replace(tzinfo=None))}**.
    """)

    # st.markdown('Choose a view from the Navigation menu on the left to get started!')

    # st.markdown("")
    chatdb_fpath = st.text_input(
        label="Where's your chat.db? For most use cases, it's in ~/Library/Messages. Use the following text input to adjust if necessary.",
        value='~/Library/Messages/chat.db',
        placeholder='~/Library/Messages/chat.db',
        help='The path to your chat.db file.',
    )

    st.session_state.chatdb_fpath = chatdb_fpath


    def refresh_app_data():
        """
        Remove the existing extract and completely re-extract data from the original chat.db.
        """
        remove_extract(logger=logger)


    if st.button(label='Refresh', help='Refreshes your iMessage data.', on_click=refresh_app_data):
        pass


    logger.info('Done', arrow='black')