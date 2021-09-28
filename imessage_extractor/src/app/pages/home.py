"""Home page shown when the user enters the application"""
import streamlit as st
import humanize
from pydoni import advanced_strip
from imessage_extractor.src.app.helpers import to_date_str, span, large_text_green, large_text, medium_text, medium_text_green


def intword(n: int) -> str:
    """
    Apply humanize.intword() and custom formatting thereafter.
    """
    str_map = dict(thousand='K', million='M', billion='B')
    word = humanize.intword(n)
    for k, v in str_map.items():
        word = word.replace(' ' + k, v)

    return word


def write(data, logger) -> None:
    """
    Write the homepage.
    """
    st.image('../../../graphics/imessage_extractor_logo.png')
    latest_ts = data.message_vw.ts.max()
    st.write(advanced_strip(f"""
    Historical statistics for your chats from **{to_date_str(data.message_vw.dt.min())}**
    until **{to_date_str(data.message_vw.dt.max())}**. Data last refreshed
    **{humanize.naturaltime(latest_ts.replace(tzinfo=None))}**.
    """))

    total_days = len(data.message_vw['dt'].unique())
    st.markdown(f'{large_text_green(total_days)} {medium_text("active texting days")}', unsafe_allow_html=True)

    total_messages = len(data.message_vw)
    st.markdown(f'{large_text_green(intword(total_messages))} {medium_text("total messages")}', unsafe_allow_html=True)

    # total_words = data.message_vw_text['n_tokens'].sum()
    # st.markdown(f'{large_text_green(intword(total_words))} {medium_text("total words")}', unsafe_allow_html=True)

    # total_letters = data.message_vw_text['n_characters'].sum()
    # st.markdown(f'{large_text_green(intword(total_letters))} {medium_text("total letters")}', unsafe_allow_html=True)
