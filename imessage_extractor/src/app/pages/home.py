"""Home page shown when the user enters the application"""
import streamlit as st
import awesome_streamlit as ast
import humanize
from pydoni import advanced_strip


def intword(n: int) -> str:
    """
    Apply humanize.intword() and custom formatting thereafter.
    """
    str_map = dict(thousand='K', million='M', billion='B')
    word = humanize.intword(n)
    for k, v in str_map.items():
        word = word.replace(' ' + k, v)

    return word


to_date_str = lambda dt: dt.strftime('%b %-d, %Y') if dt is not None else ''


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
    st.write('TOTAL DAYS')
    st.markdown(f'<p class="big-font">{intword(total_days)}</p>', unsafe_allow_html=True)

    total_messages = len(data.message_vw)
    st.write('TOTAL MESSAGES')
    st.markdown(f'<p class="big-font">{intword(total_messages)}</p>', unsafe_allow_html=True)

    total_words = data.message_vw_text['n_tokens'].sum()
    st.write('TOTAL WORDS')
    st.markdown(f'<p class="big-font">{intword(total_words)}</p>', unsafe_allow_html=True)

    total_letters = data.message_vw_text['n_characters'].sum()
    st.write('TOTAL LETTERS')
    st.markdown(f'<p class="big-font">{intword(total_letters)}</p>', unsafe_allow_html=True)
