import nltk
nltk.download('stopwords')
import streamlit as st
from imessage_extractor.src.app.helpers import to_date_str, csstext, htmlbold
from os.path import join, dirname
from imessage_extractor.src.app.pages.pick_a_contact.common import prepare_page_data, get_altair_dt_plot_attributes, controls
from imessage_extractor.src.app.pages.pick_a_contact.viz_active_texting_days import viz_active_texting_days
from imessage_extractor.src.app.pages.pick_a_contact.viz_message_volume import viz_message_volume
from imessage_extractor.src.app.pages.pick_a_contact.viz_pct_my_messages import viz_pct_my_messages
from imessage_extractor.src.app.pages.pick_a_contact.viz_tabular_chat_history import viz_tabular_chat_history
from imessage_extractor.src.app.pages.pick_a_contact.viz_top_token_usage_by_length import viz_favorite_words_by_length
from imessage_extractor.src.app.pages.pick_a_contact.viz_wordcloud import viz_wordcloud


root_dir = dirname(dirname(dirname(dirname(dirname(dirname(__file__))))))


def write(data, logger) -> None:
    """
    Write the Pick a Contact page.
    """
    logger.info('Writing page "Pick a Contact"')
    st.image(join(root_dir, 'graphics', 'pick_a_contact.png'))

    #
    # Prepare page
    #

    contact_name, dt_gran = controls(data)

    inputs, page_data, stats, message_count_col, selected_include_type_columns = prepare_page_data(data, contact_name, dt_gran)

    # Get altair plot attributes dependent on date granularity
    tooltip_dt_title, tooltip_dt_format, xaxis_identifier, dt_offset = get_altair_dt_plot_attributes(dt_gran)

    st.markdown(csstext(f'''
    First message on {htmlbold(to_date_str(stats["first_message_dt"]))},
    latest message on {htmlbold(to_date_str(stats["last_message_dt"]))}
    ''', cls='small-text'), unsafe_allow_html=True)

    #
    # Active texting days
    #

    st.markdown(csstext('Active Texting Days', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(csstext('At least one message exchanged on...', cls='small-text'), unsafe_allow_html=True)

    viz_active_texting_days(page_data=page_data, stats=stats, message_count_col=message_count_col)

    st.markdown('<br>', unsafe_allow_html=True)

    #
    # Message volume
    #

    st.markdown(csstext('Message Volume', cls='medium-text-bold', header=True), unsafe_allow_html=True)

    viz_message_volume(page_data=page_data,
                       stats=stats,
                       contact_name=contact_name,
                       message_count_col=message_count_col,
                       dt_offset=dt_offset,
                       dt_gran=dt_gran,
                       xaxis_identifier=xaxis_identifier,
                       tooltip_dt_title=tooltip_dt_title,
                       tooltip_dt_format=tooltip_dt_format)

    #
    # Percent My Messages
    #

    st.markdown(csstext('% of All My Messages', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(csstext(f'Percent of my total volume (across all contacts) made up to & from {htmlbold(contact_name)}:', cls='small-text'), unsafe_allow_html=True)

    viz_pct_my_messages(data=data,
                        page_data=page_data,
                        message_count_col=message_count_col,
                        dt_gran=dt_gran,
                        dt_offset=dt_offset,
                        selected_include_type_columns=selected_include_type_columns,
                        xaxis_identifier=xaxis_identifier,
                        tooltip_dt_title=tooltip_dt_title,
                        tooltip_dt_format=tooltip_dt_format)

    #
    # Words
    #

    st.markdown(csstext('Words', cls='medium-text-bold', header=True), unsafe_allow_html=True)

    viz_wordcloud(data=data, page_data=page_data, stats=stats, contact_name=contact_name)

    st.markdown('<br>', unsafe_allow_html=True)

    #
    # Favorite words by word length
    #

    st.markdown(csstext('Favorite Tokens', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(csstext('Most frequently used tokens by length', cls='small-text'), unsafe_allow_html=True)

    apostrophe_s = "'" if contact_name[-1] == 's' else "'s"
    st.markdown(csstext(f"{htmlbold(contact_name)}{apostrophe_s} favorites", cls='small-text'), unsafe_allow_html=True)
    viz_favorite_words_by_length(data=data, page_data=page_data, contact_name=contact_name, is_from_me=False)

    st.markdown(csstext('My favorites:', cls='small-text'), unsafe_allow_html=True)
    viz_favorite_words_by_length(data=data, page_data=page_data, contact_name=contact_name, is_from_me=True)

    #
    # Tabular message preview
    #

    st.markdown(csstext('Chat History', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(csstext(f'Snapshot of my message history with {htmlbold(contact_name)}, most recent messages first', cls='small-text'), unsafe_allow_html=True)

    viz_tabular_chat_history(data=data, inputs=inputs, contact_name=contact_name)

    #
    # Wrap up
    #

    logger.info('=> done')
