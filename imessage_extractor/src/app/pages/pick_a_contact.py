"""Pick a Contact page"""
import streamlit as st
import pandas as pd
from pydoni import advanced_strip
from imessage_extractor.src.app.helpers import to_date_str, medium_text, medium_text_green, large_text, large_text_green, span
import plotly.graph_objects as go
import altair as alt


def write(data, logger) -> None:
    """
    Write the Pick a Contact page.
    """
    logger.info('Writing page "Pick a Contact"')
    st.markdown('<p class="big-font">Pick a Contact</p>', unsafe_allow_html=True)

    # Controls
    show_group_chats = st.checkbox('Show group chats', value=False, help="Include group chat names in the 'Contact name' dropdown?")

    col1, col2 = st.columns(2)

    contact_names_display = data.lst_contact_names_all if show_group_chats else data.lst_contact_names_no_group_chats
    contact_name = col1.selectbox('Contact name', contact_names_display, contact_names_display.index('Maria Sooklaris'),
                                  help="Choose a contact you'd like to analyze data for!")

    dt_options = ['Day', 'Week', 'Month', 'Year']
    dt_gran = col2.selectbox('Date granularity', dt_options, index=dt_options.index('Month'), help='Determine the date granularity of the visualizations below')

    include_types = col2.multiselect(
        'Include message types',
        ['iMessage', 'SMS', 'Emote', 'App for iMessage'],
        ['iMessage', 'SMS', 'Emote', 'App for iMessage'],
        help=advanced_strip("""Select the message types to be included
            in the analysis below. By default, all message types are included.
            'Emote' includes all tapback replies (i.e. likes, dislikes, hearts, etc.).
            'App for iMessage' includes other messages sent via apps
            integrated into iMessage (i.e. Workout notifications, Apple Cash payments, etc.).
            """))


    def pull_contact_summary(contact_name: str) -> pd.DataFrame:
        """
        Subset the daily summary dataset for the selected contact. Return a dataframe
        aggregated at the day level.
        """
        df = (
            data.daily_summary_contact
            .loc[data.daily_summary_contact.index.get_level_values('contact_name') == contact_name]
            .droplevel('contact_name')
        )

        df.index = pd.to_datetime(df.index)
        return df


    def resample_dataframe(df: pd.DataFrame, dt_gran: str) -> pd.DataFrame:
        """
        Resample summary dataframe based on selected date granularity.
        """
        df.index = pd.to_datetime(df.index)

        if dt_gran == 'Day':
            return df
        elif dt_gran == 'Week':
            return df.resample('W-SUN').sum()
        elif dt_gran == 'Month':
            return df.resample('MS').sum()
        elif dt_gran == 'Year':
            return df.resample('AS').sum()


    # Initialize a dictionary to store user inputs used to write this page
    inputs = {}

    # Initialize a ditionary to store datasets used to write this page
    page_data = {}

    # Initialize dictionary to store high-level statistics (usually a single number or date)
    # to be written on this page
    stats = {}

    page_data['summary_day'] = pull_contact_summary(contact_name)

    # Get max and min message dates for this contact
    stats['first_message_dt'] = page_data['summary_day'].index.get_level_values('dt').min()
    stats['last_message_dt'] = page_data['summary_day'].index.get_level_values('dt').max()

    # Add filter now that first and last message dates are known
    inputs['filter_start_dt'], inputs['filter_stop_dt'] = col1.date_input(
        label='Date range',
        value=[stats['first_message_dt'], stats['last_message_dt']],
        help='Filter the date range to anaylyze data for? Defaults to the first/last dates for this contact.'
    )
    page_data['summary_day'] = page_data['summary_day'].loc[inputs['filter_start_dt']:inputs['filter_stop_dt']]

    # Days with at least one message exchanged for this contact in filter range
    stats['active_texting_days'] = page_data['summary_day'].shape[0]

    # Days with at least one message exchanged for any contact in filter range
    stats['total_active_texting_days'] = len(
        data.message_vw.loc[(data.message_vw['dt'] >= pd.to_datetime(inputs['filter_start_dt']))
        & (data.message_vw['dt'] <= pd.to_datetime(inputs['filter_stop_dt']))]['dt'].unique()
    )

    # Percent of total active days across all contacts with at least one message exchanged
    # to or from the selected contact
    stats['active_texting_days_pct'] = round(stats['active_texting_days'] / stats['total_active_texting_days'] * 100, 1)

    # Resample to selected date granularity
    page_data['summary'] = resample_dataframe(page_data['summary_day'], dt_gran)

    # Get altair plot attributes dependent on date granularity
    if dt_gran == 'Day':
        tooltip_dt_title = 'on date'
        tooltip_dt_format = '%b %d, %y'
        xaxis_identifier = 'dt'
    elif dt_gran == 'Week':
        tooltip_dt_title = 'week of'
        tooltip_dt_format = '%b %d, %y'
        xaxis_identifier = 'dt'
    elif dt_gran == 'Month':
        tooltip_dt_title = 'month of'
        tooltip_dt_format = '%b %y'
        xaxis_identifier = 'monthdate(dt):O'
    elif dt_gran == 'Year':
        tooltip_dt_title = 'year of'
        tooltip_dt_format = '%Y'
        xaxis_identifier = 'yearmonthdate(dt):O'

    st.write(f'First message on **{to_date_str(stats["first_message_dt"])}**, latest message on **{to_date_str(stats["last_message_dt"])}**')

    st.markdown('# Active Texting Days')
    st.markdown(span('At least one message exchanged on...', cls='subtitle'), unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    col1.markdown(f'{large_text_green(stats["active_texting_days"])}<br>active texting days', unsafe_allow_html=True)
    col2.markdown(f'{large_text_green(str(stats["active_texting_days_pct"]) + "%")}<br>of days in selected time window', unsafe_allow_html=True)

    st.markdown('<br>', unsafe_allow_html=True)

    st.markdown('## Message Volume')
    st.markdown(span('Count of messages for the selected contact', cls='subtitle'), unsafe_allow_html=True)

    brush = alt.selection_interval(encodings=['x'])
    st.altair_chart(
        alt.Chart(page_data['summary'].reset_index())
        .mark_bar()
        .encode(
            x=alt.X(xaxis_identifier, title=None),
            y=alt.Y('n_messages', title=None),
            color=alt.condition(brush, alt.value('#83cf83'), alt.value('lightgray')),
            tooltip=[
                alt.Tooltip('n_messages', title='Messages'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=300)
    )

    st.markdown('## % of Volume across All Contacts')
    st.markdown(span('Percent of volume across all contacts made up by the selected contact', cls='subtitle'), unsafe_allow_html=True)

    pct_message_volume = pd.concat([
            page_data['summary']['n_messages'].rename('n_messages_contact'),
            resample_dataframe(data.daily_summary, dt_gran)['n_messages'],
        ],
        axis=1
    ).fillna(0.)
    pct_message_volume['rate'] = pct_message_volume['n_messages_contact'] / pct_message_volume['n_messages']

    st.altair_chart(
        alt.Chart(pct_message_volume['rate'].reset_index())
        .mark_line(size=10)
        .encode(
            x=alt.X('dt', title=None),
            y=alt.Y('rate', title=None, axis=alt.Axis(format='%')),
            color=alt.condition(brush, alt.value('#83cf83'), alt.value('lightgray')),
            tooltip=[
                alt.Tooltip('rate', title='Percent', format='.2%'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=300)
    )

    # TODO: message types
    # TODO message preview for given date range

    logger.info('Finished writing page "Pick a Contact"')
