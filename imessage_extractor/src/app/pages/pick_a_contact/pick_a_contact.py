import nltk
nltk.download('stopwords')
import streamlit as st
from imessage_extractor.src.app.helpers import to_date_str, csstext
from os.path import join, dirname
from imessage_extractor.src.app.pages.pick_a_contact.common import prepare_page_data, get_altair_dt_plot_attributes, controls
from imessage_extractor.src.app.pages.pick_a_contact.viz_texting_activity import viz_texting_activity
from imessage_extractor.src.app.pages.pick_a_contact.viz_message_volume import viz_message_volume
from imessage_extractor.src.app.pages.pick_a_contact.viz_tabular_chat_history import viz_tabular_chat_history
from imessage_extractor.src.app.pages.pick_a_contact.viz_word_analysis import viz_word_analysis


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

    contact_name, dt_gran, show_group_chats = controls(data)

    inputs, page_data, stats, message_count_col, selected_include_type_columns = prepare_page_data(data, contact_name, dt_gran)

    # Get altair plot attributes dependent on date granularity
    tooltip_dt_title, tooltip_dt_format, xaxis_identifier, dt_offset = get_altair_dt_plot_attributes(dt_gran)

    st.markdown(f'First message on **{to_date_str(stats["first_message_dt"])}**, latest message on **{to_date_str(stats["last_message_dt"])}**.')

    #
    # Message volume
    #

    st.markdown(csstext('Message Volume', cls='medium-text-bold', header=True), unsafe_allow_html=True)

    viz_message_volume(data=data,
                       page_data=page_data,
                       contact_name=contact_name,
                       message_count_col=message_count_col,
                       dt_offset=dt_offset,
                       dt_gran=dt_gran,
                       show_group_chats=show_group_chats,
                       xaxis_identifier=xaxis_identifier,
                       tooltip_dt_title=tooltip_dt_title,
                       tooltip_dt_format=tooltip_dt_format,
                       selected_include_type_columns=selected_include_type_columns)

    #
    # Texting Activity
    #

    st.markdown(csstext('Texting Activity', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown('At least one message exchanged on...')

    viz_texting_activity(page_data=page_data, stats=stats, message_count_col=message_count_col)

    st.markdown('<br>', unsafe_allow_html=True)

    #
    # Word Analysis
    #

    viz_word_analysis(data=data, page_data=page_data, stats=stats, contact_name=contact_name)

    st.markdown('<br>', unsafe_allow_html=True)

    st.markdown(csstext('Chat History', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(f'Snapshot of my message history with **{contact_name}**, most recent messages first.')

    viz_tabular_chat_history(data=data, inputs=inputs, contact_name=contact_name)

    #
    # Wrap up
    #

    logger.info('=> done')
