import altair as alt
import streamlit as st
from imessage_extractor.src.app.helpers import intword, csstext
from imessage_extractor.src.app.pages.pick_a_contact.common import resample_dataframe


def get_point_size(xaxis_length: int) -> float:
    """
    Apply a piecewise formula to determine the optimal point size dependent on the
    number of x-axis ticks.
    """
    if xaxis_length <= 95:
        return 242.192 - 1.91781 * xaxis_length
    elif xaxis_length > 95:
        return 67.4074 - 0.0779727 * xaxis_length


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


def viz_message_volume(data,
                       page_data,
                       contact_name,
                       message_count_col,
                       dt_offset,
                       dt_gran,
                       show_group_chats,
                       xaxis_identifier,
                       tooltip_dt_title,
                       tooltip_dt_format,
                       selected_include_type_columns,
                       ) -> None:
    """
    Create the visualization
    """
    total_messages = page_data['summary_day'][message_count_col].sum()
    total_messages_from_me_pct = page_data['summary_day_from_who']['messages_from_me'].sum() / page_data['summary_day'][message_count_col].sum()
    total_messages_from_them_pct = page_data['summary_day_from_who']['messages_from_them'].sum() / page_data['summary_day'][message_count_col].sum()

    st.markdown(f"""Total messages exchanged with **{contact_name}**,
    **{str(int(round(total_messages_from_me_pct * 100, 0))) + '%'}** sent by me,
    **{str(int(round(total_messages_from_them_pct * 100, 0))) + '%'}** sent by them.
    """)

    st.markdown(csstext(intword(total_messages), cls='large-text-green-center'), unsafe_allow_html=True)
    st.markdown('<br>', unsafe_allow_html=True)

    st.markdown(f"Here's our total message volume over time, shown by **{dt_gran}**:")

    chart_df = page_data['summary'].copy()
    chart_df.index = chart_df.reset_index()['dt'] + dt_offset
    brush = alt.selection_interval(encodings=['x'])

    st.altair_chart(
        alt.Chart(data=chart_df.sort_index().reset_index(), background='#2b2b2b')
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
    # Rank contacts by message volume
    #

    chart_df = data.daily_summary_contact_from_who_vw.reset_index().groupby('contact_name').sum()
    chart_df[message_count_col] = chart_df[selected_include_type_columns].sum(axis=1)
    chart_df = chart_df[[message_count_col]].sort_values(by=message_count_col, ascending=False)

    if not show_group_chats:
        # Filter out group chats
        chart_df = chart_df.loc[chart_df.index.get_level_values('contact_name').isin(data.lst_contact_names_no_group_chats)]


    # Only show X contacts
    limit = 20
    if contact_name in chart_df.head(limit).index:
        # Straightforward limit since the contact is in the top `limit` rows
        chart_df = chart_df.head(limit)
    else:
        # Take the top `limit-1` contacts, skip the rest, then show the selected
        # contact all the way at the end
        tmp_df = chart_df.head(limit).copy()
        tmp_df.loc[contact_name] = chart_df.loc[contact_name].squeeze()
        chart_df = tmp_df.copy()
        del tmp_df


    st.altair_chart(
        alt.Chart(data=chart_df.reset_index(), background='#2b2b2b')
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(chart_df)),
            cornerRadiusTopRight=get_corner_radius_size(len(chart_df))
        ).encode(
            x=alt.X('contact_name', title=None, sort=list(chart_df.index), axis=alt.Axis(labelColor='dimgray')),
            y=alt.Y(message_count_col, title=None, axis=alt.Axis(labelColor='dimgray')),
            color=alt.condition(
                alt.datum.contact_name == contact_name,
                alt.value('#83cf83'),
                alt.value('dimgray')
            ),
            tooltip=[
                alt.Tooltip(message_count_col, title='Messages'),
                alt.Tooltip('contact_name', title='Contact'),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=450)
    )

    #
    # Gradient area chart showing message volume per day of the week
    #

    tmp_df = page_data['summary_day'][[message_count_col]].reset_index().copy()
    tmp_df['weekday'] = tmp_df['dt'].dt.day_name()
    tmp_df = tmp_df.drop('dt', axis=1).groupby('weekday').mean()
    most_popular_day = tmp_df[message_count_col].idxmax()
    n_messages_on_most_popular_day = tmp_df[message_count_col].max()

    st.markdown("Here's the average number of messages we've exchanged per day of the week:")

    st.altair_chart(
        alt.Chart(data=tmp_df.reset_index(), background='#2b2b2b')
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

    st.markdown(f'Looks like **{most_popular_day}** is our most popular texting day, with an average of **{round(n_messages_on_most_popular_day, 1)}** messages exchanged that day.')

    #
    # Percent of All My Messages
    #

    st.markdown(csstext('% of All My Messages', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(f'Percent of my total volume (across all contacts) made up to & from **{contact_name}**:')

    pct_message_volume = (
        page_data['summary']
        [message_count_col]
        .rename('messages_contact')
        .reset_index()
        .merge(resample_dataframe(data.daily_summary_vw
                                  [selected_include_type_columns]
                                  .sum(axis=1),
                                  dt_gran)
               .rename('messages_all_contacts')
               .reset_index()
               [['dt', 'messages_all_contacts']],
               on='dt')
    )

    pct_message_volume['rate'] = pct_message_volume['messages_contact'] / pct_message_volume['messages_all_contacts']

    chart_df = pct_message_volume.copy()
    chart_df.index = chart_df.reset_index()['dt'] + dt_offset
    brush = alt.selection_interval(encodings=['x'])

    st.altair_chart(
        alt.Chart(data=chart_df['rate'].sort_index().reset_index(), background='#2b2b2b')
        .mark_line(size=3, point=dict(filled=False, fill='darkslategray'))
        .encode(
            x=alt.X(xaxis_identifier, title=None, axis=alt.Axis(format=tooltip_dt_format, labelColor='dimgray')),
            y=alt.Y('rate', title=None, axis=alt.Axis(format='%', labelColor='dimgray')),
            color=alt.condition(brush, alt.value('#83cf83'), alt.value('lightgray')),
            tooltip=[
                alt.Tooltip('rate', title='Percent', format='.2%'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .configure_point(size=get_point_size(len(chart_df)))
        .add_selection(brush)
        .properties(width=600, height=300)
    )
