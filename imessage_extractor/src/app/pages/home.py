from imessage_extractor.src.app.helpers import to_date_str, intword, csstext, htmlbold
from os.path import dirname, join
import streamlit as st
import humanize


root_dir = dirname(dirname(dirname(dirname(dirname(__file__)))))


def write(data, logger) -> None:
    """
    Write the homepage.
    """
    st.image(join(root_dir, 'graphics', 'imessage_extractor_logo.png'))
    latest_ts = data.message_user.ts.max()

    st.markdown(f"""
    Statistics on my iMessage history from
    **{to_date_str(data.message_user.dt.min())}**
    until
    **{to_date_str(data.message_user.dt.max())}.**
    Data last refreshed
    **{humanize.naturaltime(latest_ts.replace(tzinfo=None))}**.
    """)

    col1, col2 = st.columns((1.4, 1))

    total_days = len(data.message_user['dt'].unique())
    col1.markdown(csstext(total_days, cls='large-text-green-center'), unsafe_allow_html=True)
    col1.markdown(csstext('active texting days', cls='small-text-center'), unsafe_allow_html=True)
    col1.markdown('<br><br>', unsafe_allow_html=True)

    total_messages = len(data.message_user)
    col1.markdown(csstext(intword(total_messages), cls='large-text-green-center'), unsafe_allow_html=True)
    col1.markdown(csstext('total messages', cls='small-text-center'), unsafe_allow_html=True)
    col1.markdown('<br><br>', unsafe_allow_html=True)

    total_words = data.message_user_text_vw['n_tokens'].sum()
    col2.markdown(csstext(intword(total_words), cls='large-text-green-center'), unsafe_allow_html=True)
    col2.markdown(csstext('total words', cls='small-text-center'), unsafe_allow_html=True)
    col2.markdown('<br><br>', unsafe_allow_html=True)

    total_letters = data.message_user_text_vw['n_characters'].sum()
    col2.markdown(csstext(intword(total_letters), cls='large-text-green-center'), unsafe_allow_html=True)
    col2.markdown(csstext('total letters', cls='small-text-center'), unsafe_allow_html=True)
