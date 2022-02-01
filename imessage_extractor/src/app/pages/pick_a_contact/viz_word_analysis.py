import pandas as pd
import streamlit as st
import stylecloud
import altair as alt
import plotly.express as px
from imessage_extractor.src.app.helpers import intword, csstext
from nltk.corpus import stopwords
from os import remove
from os.path import isfile


def viz_word_analysis(data, page_data, stats, contact_name) -> None:
    """
    Create the visualization.
    """
    st.markdown(csstext('Words', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(f'A visual representation of the words used in conversation with **{contact_name}**.')

    #
    # Total tokens
    #

    total_tokens = page_data['summary_day']['tokens'].sum()
    total_tokens_from_me_pct = page_data['summary_day_from_who']['tokens_from_me'].sum() / page_data['summary_day']['tokens'].sum()
    total_tokens_from_them_pct = page_data['summary_day_from_who']['tokens_from_them'].sum() / page_data['summary_day']['tokens'].sum()

    st.markdown(csstext(intword(total_tokens), cls='large-text-green-center'), unsafe_allow_html=True)

    st.markdown(f"""Total words exchanged with **{contact_name}**,
    **{str(int(round(total_tokens_from_them_pct * 100, 0))) + '%'}** written by them,
    **{str(int(round(total_tokens_from_me_pct * 100, 0))) + '%'}** written by me.
    """)

    st.markdown('<br>', unsafe_allow_html=True)

    #
    # Wordcloud
    #

    # Pull data
    df_word_counts = (
        data.contact_token_usage_from_who_vw
        .reset_index()
        .loc[data.contact_token_usage_from_who_vw.reset_index()['contact_name'] == contact_name]
        [['is_from_me', 'token', 'usages']]
        .sort_values(['is_from_me', 'usages'], ascending=False)
    )

    # Filter stopwords and punctuation
    df_word_counts['token'] = df_word_counts['token'].str.lower()
    df_word_counts['token'] = (
        df_word_counts['token']
        .str.replace(r'[^\w\s]+', '', regex=True)
        .str.replace(r'^(s|d)$', '', regex=True)
    )
    df_word_counts = df_word_counts.loc[df_word_counts['token'] > '']

    expected_stylecloud_from_me_fpath = 'stylecloud_from_me.png'
    expected_stylecloud_from_them_fpath = 'stylecloud_from_them.png'

    for is_from_me in [True, False]:
        tmp_wordcloud_fpath = 'tmp_imessage_extractor_app_pick_a_contact_wordcloud.csv'
        (
            df_word_counts
            .loc[df_word_counts['is_from_me'] == is_from_me]
            [['token', 'usages']]
            .head(500)
            .to_csv(tmp_wordcloud_fpath, index=False)
        )

        if is_from_me:
            expected_stylecloud_fpath = expected_stylecloud_from_me_fpath
            icon_name = 'fas fa-comment fa-flip-horizontal'  # FIXME - this is horizontal flip is not working
        else:
            expected_stylecloud_fpath = expected_stylecloud_from_them_fpath
            icon_name = 'fas fa-comment'

        stylecloud.gen_stylecloud(file_path=tmp_wordcloud_fpath,
                                  icon_name=icon_name,
                                  colors=['#83cf83', '#a6e0a6', '#dcecdc', '#ffffff'],
                                  background_color='#2b2b2b',
                                  output_name=expected_stylecloud_fpath,
                                  gradient='horizontal',
                                  max_words=500,
                                  stopwords=True,
                                  custom_stopwords=data.contractions_wo_apostrophe + stopwords.words('english'),
                                  size=(1024, 800),)

    col1, col2 = st.columns(2)
    col1.markdown(csstext(contact_name, cls='smallmedium-text-bold-center'), unsafe_allow_html=True)
    col2.markdown(csstext('Me', cls='smallmedium-text-bold-center'), unsafe_allow_html=True)

    st.markdown(csstext('Favorite words', cls='small22-text-center'), unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    if isfile(expected_stylecloud_from_them_fpath):
        col1.image(expected_stylecloud_from_them_fpath)
        remove(expected_stylecloud_from_them_fpath)
    else:
        col1.markdown(csstext('Wordcloud not available 😔', cls='medium-text-center'), unsafe_allow_html=True)

    if isfile(expected_stylecloud_from_me_fpath):
        col2.image(expected_stylecloud_from_me_fpath)
        remove(expected_stylecloud_from_me_fpath)
    else:
        col2.markdown(csstext('Wordcloud not available 😔', cls='medium-text-center'), unsafe_allow_html=True)

    if isfile(tmp_wordcloud_fpath):
        remove(tmp_wordcloud_fpath)

    #
    # Favorite tokens by word length
    #

    st.markdown(csstext('Favorite tokens by word length', cls='small22-text-center'), unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    token_usage_from_who_ranked_by_length = (
        data.contact_token_usage_from_who_vw
        .loc[data.contact_token_usage_from_who_vw.index.get_level_values('contact_name') == contact_name]
        .droplevel('contact_name')
        .reset_index()
    )

    token_usage_from_who_ranked_by_length['token'] = token_usage_from_who_ranked_by_length['token'].str.lower()

    # Filter out stopwords, punctuation characters and contractions
    token_usage_from_who_ranked_by_length = token_usage_from_who_ranked_by_length.loc[
        (~token_usage_from_who_ranked_by_length['token'].isin(stopwords.words('english')))
        # & (~token_usage_from_who_ranked_by_length['token'].isin(data.lst_punctuation_chars))
        & (~token_usage_from_who_ranked_by_length['token'].isin(data.lst_contractions_w_apostrophe))
        & (~token_usage_from_who_ranked_by_length['token'].str.isdigit().astype(bool))
    ]

    token_usage_from_who_ranked_by_length['rank'] = (
        token_usage_from_who_ranked_by_length
        .groupby(['is_from_me', 'length'])
        ['usages']
        .rank(method='first', ascending=False)
    )

    page_data['top_token_usage_from_who_by_length'] = (
        token_usage_from_who_ranked_by_length
        .loc[
            (token_usage_from_who_ranked_by_length['rank'] == 1)
            & (token_usage_from_who_ranked_by_length['length'] <= 10)
        ]
        .drop('rank', axis=1)
        .sort_values(['is_from_me', 'length'])
    )

    brush = alt.selection_interval(encodings=['y'])

    df_chart_from_me = (
        page_data['top_token_usage_from_who_by_length']
        .loc[page_data['top_token_usage_from_who_by_length']['is_from_me'] == True]
        .sort_values('length')
    )

    df_chart_from_them = (
        page_data['top_token_usage_from_who_by_length']
        .loc[page_data['top_token_usage_from_who_by_length']['is_from_me'] == False]
        .sort_values('length')
    )

    col1, col2 = st.columns(2)

    col1.altair_chart(
        alt.Chart(data=df_chart_from_them, background='#2b2b2b')
        .mark_bar(size=3, point=dict(filled=False, fill='darkslategray'))
        .mark_bar(
            cornerRadiusTopRight=3,
            cornerRadiusBottomRight=3
        )
        .encode(
            x=alt.X('usages', title=None, axis=alt.Axis(labelColor='dimgray')),
            y=alt.Y('token', title=None, sort=list(df_chart_from_them['token']), axis=alt.Axis(labelColor='dimgray')),
            color=alt.condition(brush, alt.value('#83cf83'), alt.value('gray')),
            tooltip=[
                alt.Tooltip('token', title='Token'),
                alt.Tooltip('length', title='Length'),
                alt.Tooltip('usages', title='Usages'),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=300, height=300),
        use_container_width=False
    )

    col2.altair_chart(
        alt.Chart(data=df_chart_from_me, background='#2b2b2b')
        .mark_bar(size=3, point=dict(filled=False, fill='darkslategray'))
        .mark_bar(
            cornerRadiusTopRight=3,
            cornerRadiusBottomRight=3
        )
        .encode(
            x=alt.X('usages', title=None, axis=alt.Axis(labelColor='dimgray')),
            y=alt.Y('token', title=None, sort=list(df_chart_from_me['token']), axis=alt.Axis(labelColor='dimgray')),
            color=alt.condition(brush, alt.value('#83cf83'), alt.value('gray')),
            tooltip=[
                alt.Tooltip('token', title='Token'),
                alt.Tooltip('length', title='Length'),
                alt.Tooltip('usages', title='Usages'),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=300, height=300),
        use_container_width=False
    )

    #
    # Average words per message
    #

    avg_words_per_message_from_me = \
        (page_data['summary_day_from_who'].sum()['tokens_from_me']
         / page_data['summary_day_from_who'].sum()['text_messages_from_me'])

    avg_words_per_message_from_them = \
        (page_data['summary_day_from_who'].sum()['tokens_from_them']
         / page_data['summary_day_from_who'].sum()['text_messages_from_them'])

    st.markdown(csstext('Average words per message', cls='small22-text-center'), unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    col1.markdown(csstext(round(avg_words_per_message_from_them, 1), cls='large-text-green-center'), unsafe_allow_html=True)
    col2.markdown(csstext(round(avg_words_per_message_from_me, 1), cls='large-text-green-center'), unsafe_allow_html=True)

    #
    # Average characters per message
    #

    avg_chars_per_message_from_me = \
        (page_data['summary_day_from_who'].sum()['characters_from_me']
         / page_data['summary_day_from_who'].sum()['text_messages_from_me'])

    avg_chars_per_message_from_them = \
        (page_data['summary_day_from_who'].sum()['characters_from_them']
         / page_data['summary_day_from_who'].sum()['text_messages_from_them'])

    st.markdown(csstext('Average characters per message', cls='small22-text-center'), unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    col1.markdown(csstext(int(round(avg_chars_per_message_from_them, 0)), cls='large-text-green-center'), unsafe_allow_html=True)
    col2.markdown(csstext(int(round(avg_chars_per_message_from_me, 0)), cls='large-text-green-center'), unsafe_allow_html=True)

    #
    # Tapbacks
    #

    df_tapback = (
        data.message_user
        .reset_index()
        .loc[
            (data.message_user.reset_index()['contact_name'] == contact_name)
            & (data.message_user.reset_index()['is_emote'] == True)
        ]
        .groupby(['is_from_me', 'message_special_type'])
        .agg({'message_id': 'count'})
        .reset_index()
        .rename(columns={'message_id': 'count', 'message_special_type': 'tapback'})
    )
    df_tapback['tapback'] = df_tapback['tapback'].replace(data.tapback_type_map)

    all_tapbacks = list(data.tapback_type_map.values())
    df_tapback_from_me = (
        df_tapback
        .loc[df_tapback['is_from_me'] == True]
        .drop('is_from_me', axis=1)
        .merge(pd.DataFrame(all_tapbacks, columns=['tapback']), how='outer', on='tapback')
        .fillna(0.0)
    )
    df_tapback_from_them = (
        df_tapback
        .loc[df_tapback['is_from_me'] == False]
        .drop('is_from_me', axis=1)
        .merge(pd.DataFrame(all_tapbacks, columns=['tapback']), how='outer', on='tapback')
        .fillna(0.0)
    )

    # Color ramp generator: https://www.geeksforgeeks.org/pie-plot-using-plotly-in-python/
    color_map = {
        'Love': '#CDE7D2',
        'Like': '#AEDFB2',
        'Dislike': '#8FD792',
        'Laugh': '#70CF72',
        'Emphasis': '#51C752',
        'Question': '#32C032',
        'Remove Heart': '#DFCBEF',
        'Remove Like': '#D6BAE8',
        'Remove Dislike': '#CDA9E2',
        'Remove Laugh': '#C498DC',
        'Remove Emphasis': '#BB87D6',
        'Remove Question': '#B377D0',
    }

    fig = px.pie(
        df_tapback_from_them,
        hole=.5,
        values='count',
        names='tapback',
        color_discrete_sequence=list(color_map.values()),
        width=360,
        height=360,
    )

    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='Tapback: <b>%{label}</b><br>Usages: <b>%{value}</b>',
    )

    fig.update_layout(
        showlegend=False,
    )

    st.markdown(csstext('Favorite tapbacks', cls='small22-text-center'), unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    col1.plotly_chart(fig)

    fig = px.pie(
        df_tapback_from_me,
        hole=.5,
        values='count',
        names='tapback',
        color_discrete_sequence=list(color_map.values()),
        width=360,
        height=360,
    )

    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='Tapback: <b>%{label}</b><br>Usages: <b>%{value}</b>',
    )

    fig.update_layout(
        showlegend=False,
    )

    col2.plotly_chart(fig)

