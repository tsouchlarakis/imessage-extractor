import pandas as pd
import streamlit as st
import stylecloud
import altair as alt
import plotly.express as px
from imessage_extractor.src.app.helpers import intword, csstext
from nltk.corpus import stopwords
from os import remove
from os.path import isfile


def viz_word_analysis(data, self.pdata, stats, contact_name) -> None:
    """
    Create the visualization.
    """
    #
    # Favorite tokens by word length
    #

    #
    # Average words per message
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
    df_tapback['tapback'] = df_tapback['tapback'].replace(data.map_tapback_type)

    all_tapbacks = list(data.map_tapback_type.values())
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

