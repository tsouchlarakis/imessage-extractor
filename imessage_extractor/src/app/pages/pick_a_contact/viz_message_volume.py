import streamlit as st
import altair as alt
from imessage_extractor.src.app.helpers import intword, csstext, htmlbold


def viz_message_volume(page_data, stats, contact_name, message_count_col, dt_offset, dt_gran, xaxis_identifier, tooltip_dt_title, tooltip_dt_format):
    """
    Create the visualization
    """
    stats['total_messages'] = page_data['summary_day']['messages'].sum()
    stats['total_messages_from_me_pct'] = page_data['summary_day_from_who']['messages_from_me'].sum() / page_data['summary_day']['messages'].sum()
    stats['total_messages_from_them_pct'] = page_data['summary_day_from_who']['messages_from_them'].sum() / page_data['summary_day']['messages'].sum()

    st.markdown(csstext(intword(stats['total_messages']), cls='large-text-green'), unsafe_allow_html=True)
    st.markdown(csstext(f"""
    Total messages exchanged.
    {htmlbold(str(int(round(stats['total_messages_from_me_pct'] * 100, 0))) + '%')} sent by me,
    {htmlbold(str(int(round(stats['total_messages_from_them_pct'] * 100, 0))) + '%')} sent by {htmlbold(contact_name)}.
    """, cls='small-text') , unsafe_allow_html=True)
    st.markdown('<br>', unsafe_allow_html=True)


    def get_corner_radius_size(xaxis_length: int) -> float:
        """
        Determine the optimal corner radius size dependent on the number of x-axis ticks.
        """
        if xaxis_length <= 50:
            return 3
        elif xaxis_length > 50 and xaxis_length <= 100:
            return 2
        else:
            return 1


    st.markdown(csstext(f"Here's a plot of our total message volume over time, shown by {htmlbold(dt_gran)}:", cls='small-text'), unsafe_allow_html=True)

    chart_df = page_data['summary'].copy()
    chart_df.index = chart_df.reset_index()['dt'] + dt_offset
    brush = alt.selection_interval(encodings=['x'])

    st.altair_chart(
        alt.Chart(chart_df.sort_index().reset_index())
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(chart_df)),
            cornerRadiusTopRight=get_corner_radius_size(len(chart_df))
        ).encode(
            x=alt.X(xaxis_identifier, title=None, axis=alt.Axis(format=tooltip_dt_format, labelColor='dimgray')),
            y=alt.Y(message_count_col, title=None, axis=alt.Axis(labelColor='dimgray')),
            color=alt.condition(brush, alt.value('#83cf83'), alt.value('gray')),
            tooltip=[
                alt.Tooltip(message_count_col, title='Messages'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=300)
    )

    #
    # Gradient area chart showing message volume per day of the week
    #

    tmp_df = page_data['summary_day'][[message_count_col]].reset_index().copy()
    tmp_df['weekday'] = tmp_df['dt'].dt.day_name()
    tmp_df = tmp_df.drop('dt', axis=1).groupby('weekday').mean()
    most_popular_day = tmp_df[message_count_col].idxmax()

    st.markdown(csstext(f"Here's the average number of messages we've exchanged per day of the week:", cls='small-text') , unsafe_allow_html=True)

    st.altair_chart(
        alt.Chart(tmp_df.reset_index())
        .mark_area(
            color=alt.Gradient(
                gradient='linear',
                stops=[
                    alt.GradientStop(color='white', offset=0),
                    alt.GradientStop(color='lightgreen', offset=1)
                ],
                x1=1, x2=1, y1=1, y2=0
            )
        ).encode(
            x=alt.X('weekday',
                    title=None,
                    sort=['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
                    axis=alt.Axis(labelColor='dimgray')),
            y=alt.Y(message_count_col, title=None, axis=alt.Axis(labelColor='dimgray')),
            tooltip=[
                alt.Tooltip(message_count_col, title='Average messages', format='.1f'),
                alt.Tooltip('weekday', title='Weekday'),
            ]
        )
        .configure_axis(grid=False)
        .configure_axisX(labelAngle=0)
        .configure_view(strokeOpacity=0)
        .properties(width=600, height=150)
    )

    st.markdown(csstext(f'Looks like {htmlbold(most_popular_day)} is our most popular texting day', cls='small-text'), unsafe_allow_html=True)
