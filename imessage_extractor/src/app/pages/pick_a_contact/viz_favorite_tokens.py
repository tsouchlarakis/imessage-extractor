import altair as alt
import streamlit as st
from nltk.corpus import stopwords


def viz_favorite_words_by_length(data, page_data, contact_name, is_from_me) -> None:
    """
    Create the visualization.
    """
    token_usage_from_who_ranked_by_length = (
        data.contact_token_usage_from_who
        .loc[data.contact_token_usage_from_who.index.get_level_values('contact_name') == contact_name]
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

    chart_df = (
        page_data['top_token_usage_from_who_by_length']
        .loc[page_data['top_token_usage_from_who_by_length']['is_from_me'] == is_from_me]
        .sort_values('length')
    )

    st.altair_chart(
        alt.Chart(chart_df)
        .mark_bar(size=3, point=dict(filled=False, fill='darkslategray'))
        .mark_bar(
            cornerRadiusTopRight=3,
            cornerRadiusBottomRight=3
        )
        .encode(
            x=alt.X('usages', title=None, axis=alt.Axis(labelColor='dimgray')),
            y=alt.Y('token', title=None, sort=list(chart_df['token']), axis=alt.Axis(labelColor='dimgray')),
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
        .properties(width=600, height=300)
    )
