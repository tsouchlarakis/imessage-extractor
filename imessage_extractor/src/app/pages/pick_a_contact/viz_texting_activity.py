import streamlit as st
import altair as alt
from imessage_extractor.src.app.helpers import csstext

def viz_texting_activity(page_data, stats, message_count_col):
    """
    Create the visualization.
    """
    col1, col2 = st.columns(2)

    col1.markdown(csstext(str(stats['active_texting_days']), cls='large-text-green-center'), unsafe_allow_html=True)
    col2.markdown(csstext(str(stats['active_texting_days_pct']) + '%', cls='large-text-green-center'), unsafe_allow_html=True)
    col1.markdown(csstext('active texting days', cls='small-text-center'), unsafe_allow_html=True)
    col2.markdown(csstext('of days in selected time window', cls='small-text-center'), unsafe_allow_html=True)

    top_active_days = page_data['summary_day'][message_count_col].sort_values(ascending=False).head(10).reset_index().reset_index()

    st.markdown('<br>', unsafe_allow_html=True)
    st.markdown('Here are the top 10 days with our highest message volume (a larger circle means more messages were exchanged on that day):')

    st.altair_chart(
        alt.Chart(data=top_active_days.reset_index(), background='#2b2b2b')
        .mark_circle(filled=False)
        .encode(
            x=alt.X('dt', title=None, axis=alt.Axis(labelColor='dimgray')),
            size=alt.Size(
                f'{message_count_col}:N',
                legend=None,
                scale=alt.Scale(range=[100, 4000]),
            ),
            color=alt.value('#83cf83'),
            tooltip=[
                alt.Tooltip(message_count_col, title='Messages'),
                alt.Tooltip('index', title='Rank'),
                alt.Tooltip('dt', title='Date', format='%b %-d, %y'),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .properties(width=600, height=150)
        .configure_mark(
            opacity=0.75,
        )
    )
