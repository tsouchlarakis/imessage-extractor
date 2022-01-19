import altair as alt
import streamlit as st
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


def viz_pct_my_messages(data,
                        page_data,
                        message_count_col,
                        dt_gran,
                        dt_offset,
                        selected_include_type_columns,
                        xaxis_identifier,
                        tooltip_dt_title,
                        tooltip_dt_format) -> None:
    """
    Create the visualization.
    """
    pct_message_volume = (
        page_data['summary']
        [message_count_col]
        .rename('messages_contact')
        .reset_index()
        .merge(resample_dataframe(data.daily_summary
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
