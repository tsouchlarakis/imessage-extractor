import altair as alt
import pandas as pd
from imessage_extractor.src.app.pages.pick_a_contact.common import resample_dataframe
import streamlit as st
from imessage_extractor.src.helpers.utils import strip_ws
from os.path import dirname, join
import datetime
from imessage_extractor.src.app.helpers import intword, csstext


root_dir = dirname(dirname(dirname(dirname(dirname(__file__)))))


def pull_page_data(data) -> dict:
    """
    Pull necessary data for the My Stats page.
    """
    pdata = {}

    pdata['summary_day'] = data.daily_summary.reset_index()
    pdata['summary_day_from_who'] = data.daily_summary_from_who.reset_index()

    pdata['summary_day']['dt'] = pd.to_datetime(pdata['summary_day']['dt'])
    pdata['summary_day_from_who']['dt'] = pd.to_datetime(pdata['summary_day_from_who']['dt'])

    return pdata


def filter_page_data(pdata: dict, filter_start_dt: datetime.datetime, filter_stop_dt: datetime.datetime) -> dict:
    """
    Apply controls filters to page data.
    """
    pdata['summary_day'] = (
        pdata['summary_day']
        .loc[
            (pdata['summary_day']['dt'] >= pd.to_datetime(filter_start_dt))
            & (pdata['summary_day']['dt'] <= pd.to_datetime(filter_stop_dt))
        ]
    )

    if len(pdata['summary_day']) == 0:
        raise ValueError('Dataframe `summary_day` has no records, there must be a mistake!')

    pdata['summary_day_from_who'] = (
        pdata['summary_day_from_who']
        .loc[
            (pdata['summary_day_from_who']['dt'] >= pd.to_datetime(filter_start_dt))
            & (pdata['summary_day_from_who']['dt'] <= pd.to_datetime(filter_stop_dt))
        ]
    )

    if len(pdata['summary_day_from_who']) == 0:
        raise ValueError('Dataframe `summary_day_from_who` has no records, there must be a mistake!')

    return pdata


def resample_page_data(pdata: dict, dt_gran: str) -> pd.DataFrame:
    """
    Resample page dataframes based on selected date granularity.
    """
    assert dt_gran in ['Day', 'Week', 'Month', 'Year'], f'Invalid date granularity: {dt_gran}'

    if dt_gran == 'Day':
        pdata['summary_resample'] = pdata['summary_day']
        pdata['summary_resample_from_who'] = pdata['summary_day_from_who']
        return pdata

    elif dt_gran == 'Week':
        resample_identifier = 'W-SUN'

    elif dt_gran == 'Month':
        resample_identifier = 'MS'

    elif dt_gran == 'Year':
        resample_identifier = 'AS'

    pdata['summary_resample'] = pdata['summary_day'].set_index('dt').resample(resample_identifier).sum().reset_index()
    pdata['summary_resample_from_who'] = pdata['summary_day_from_who'].set_index('dt').resample(resample_identifier).sum().reset_index()

    return pdata


def adaptive_date_input(use_exact_dates: bool, first_message_dt: datetime.datetime, last_message_dt: datetime.datetime) -> tuple:
    """
    Create a date input dependent on the use exact dates checkbox value.
    """
    col1, col2 = st.columns(2)

    if use_exact_dates:
        filter_start_dt = col1.date_input(
            label='Start date',
            value=first_message_dt,
            help='''Set the beginning of the date range to display data for. Defaults to the
            earliest date that at least one message was exchanged with this contact.'''
        )
        filter_stop_dt = col2.date_input(
            label='End date',
            value=last_message_dt,
            help='''Set the end of the date range to display data for. Defaults to the
            last date that at least one message was exchanged with this contact.'''
        )

    else:
        filter_stop_dt = datetime.date.today()

        relative_date_map = {
            'Today': datetime.date.today() - datetime.timedelta(days=0),
            'This week': datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday()) - datetime.timedelta(days=1),
            'This month': datetime.date.today().replace(day=1),
            'This year': datetime.datetime.now().date().replace(month=1, day=1),
            'Last 60 days': datetime.date.today() - datetime.timedelta(days=60),
            'Last 90 days': datetime.date.today() - datetime.timedelta(days=90),
            'Last 6 months': datetime.date.today() - datetime.timedelta(days=6*30),
            'Last 12 months': datetime.date.today() - datetime.timedelta(days=365),
            'All time': first_message_dt,
        }

        selected_stop_dt = st.selectbox(
            label='Date filter',
            options=relative_date_map.keys(),
            index=list(relative_date_map.keys()).index('All time'),
            help='Determine the date granularity of the visualizations below'
        )

        filter_start_dt = relative_date_map[selected_stop_dt]

    return filter_start_dt, filter_stop_dt


def message_type_input(pdata: dict) -> list:
    """
    Add message type filter.
    """
    include_types_options = ['Text', 'Emote', 'Attachment', 'URL', 'App for iMessage']
    include_types_columns = ['text_messages', 'emotes', 'messages_attachments_only', 'urls', 'app_for_imessage']
    include_types = st.multiselect(
        'Include message types',
        include_types_options,
        include_types_options,
        help=strip_ws("""Select the message types to be included
            in the analysis below. By default, all message types are included.
            'Text' includes both iMessage and SMS messages.
            'Emote' includes all tapback replies (i.e. likes, dislikes, hearts, etc.).
            'Attachment' includes messages that were ONLY attachments (photos, videos, etc.),
            i.e. not those containing text + attachments, which are already captured with 'Text'
            'App for iMessage' includes other messages sent via apps
            integrated into iMessage (i.e. Workout notifications, Apple Cash payments, etc.).
            """))

    # Apply message type filter
    selected_include_type_columns = []
    if 'Text' in include_types:
        selected_include_type_columns.append('text_messages')
    if 'Emote' in include_types:
        selected_include_type_columns.append('emotes')
    if 'Attachment' in include_types:
        selected_include_type_columns.append('messages_attachments_only')
    if 'URL' in include_types:
        selected_include_type_columns.append('urls')
    if 'App for iMessage' in include_types:
        # Includes workout notifications and Apple Cash
        selected_include_type_columns.append('app_for_imessage')

    return selected_include_type_columns


def get_altair_dt_plot_attributes(dt_gran: str) -> tuple:
    """
    Map date granularity to Altair plot attribute values
    """
    if dt_gran == 'Day':
        tooltip_dt_title = 'on date'
        tooltip_dt_format = '%b %-d, %y'
        xaxis_identifier = 'dt:T'
        dt_offset = pd.DateOffset(days=1)

    elif dt_gran == 'Week':
        tooltip_dt_title = 'week of'
        tooltip_dt_format = '%b %-d, %y'
        xaxis_identifier = 'dt:T'
        dt_offset = pd.DateOffset(weeks=1)

    elif dt_gran == 'Month':
        tooltip_dt_title = 'month of'
        tooltip_dt_format = '%b %y'
        xaxis_identifier = 'yearmonth(dt):O'
        dt_offset = pd.DateOffset(months=1)

    elif dt_gran == 'Year':
        tooltip_dt_title = 'year of'
        tooltip_dt_format = '%Y'
        xaxis_identifier = 'year(dt):O'
        dt_offset = pd.DateOffset(years=1)

    return tooltip_dt_title, tooltip_dt_format, xaxis_identifier, dt_offset


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


def write(data, logger):
    """
    Write the My Stats page.
    """
    logger.info('Writing page "My Stats"')
    st.image(join(root_dir, 'graphics', 'my_stats.png'))

    #
    # Page preparation
    #

    # Pull page data
    pdata = pull_page_data(data)

    # Add date granularity selection
    dt_options = ['Day', 'Week', 'Month', 'Year']
    dt_gran = st.selectbox(
        label='Date granularity',
        options=dt_options,
        index=dt_options.index('Month'),
        help='Determine the date granularity of the visualizations below'
    )

    # Get max and min message dates
    first_message_dt = pdata['summary_day']['dt'].min()
    last_message_dt = pdata['summary_day']['dt'].max()

    # Add filters now that first and last message dates are known
    use_exact_dates = st.checkbox(
        label='Use exact dates',
        value=False,
        help="Show date filter as a relative date dropdown select list, instead of specific start/stop date inputs?"
    )

    # Create adaptive date input
    filter_start_dt, filter_stop_dt = adaptive_date_input(use_exact_dates, first_message_dt, last_message_dt)

    # Filter page data
    pdata = filter_page_data(pdata, filter_start_dt, filter_stop_dt)

    # Add control for types of messages to display
    selected_include_type_columns = message_type_input(pdata)

    # Add column that reflects sum of selected message types
    count_col = 'display_message_count'
    pdata['summary_day'][count_col] = pdata['summary_day'][selected_include_type_columns].sum(axis=1)
    pdata['summary_day_from_who'][count_col] = pdata['summary_day_from_who'][selected_include_type_columns].sum(axis=1)

    # Add a dataframe called 'summary_resample' to page data dictionary based on selected date granularity
    pdata = resample_page_data(pdata, dt_gran)

    # Clean up variables not used in the rest of the function
    del dt_options, use_exact_dates, filter_start_dt, filter_stop_dt, selected_include_type_columns

    #
    # Message volume
    #

    st.markdown(csstext('Message Volume', cls='medium-text-bold', header=True), unsafe_allow_html=True)

    # Large text reflecting message volume
    total_messages_from_me_pct = pdata['summary_day_from_who'].loc[pdata['summary_day_from_who']['is_from_me'] == True][count_col].sum() \
        / pdata['summary_day_from_who'][count_col].sum()
    total_messages_from_others_pct = pdata['summary_day_from_who'].loc[pdata['summary_day_from_who']['is_from_me'] == False][count_col].sum() \
        / pdata['summary_day_from_who'][count_col].sum()
    st.markdown(f"""Total messages exchanged with all contacts,
    **{str(int(round(total_messages_from_me_pct * 100, 0))) + '%'}** sent by me,
    **{str(int(round(total_messages_from_others_pct * 100, 0))) + '%'}** sent by others.
    """)

    total_messages = pdata['summary_day'][count_col].sum()
    st.markdown(csstext(intword(total_messages), cls='large-text-green-center'), unsafe_allow_html=True)

    del total_messages, total_messages_from_me_pct, total_messages_from_others_pct

    # Plot of message volume over time
    tooltip_dt_title, tooltip_dt_format, xaxis_identifier, dt_offset = get_altair_dt_plot_attributes(dt_gran)
    st.markdown(f"Here's my total message volume over time, shown by **{dt_gran}**:")

    chart_df = pdata['summary_resample'].copy()
    chart_df['dt'] = chart_df['dt'] + dt_offset
    brush = alt.selection_interval(encodings=['x'])

    st.altair_chart(
        alt.Chart(data=chart_df.sort_values('dt'), background='#2b2b2b')
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(chart_df)),
            cornerRadiusTopRight=get_corner_radius_size(len(chart_df))
        ).encode(
            x=alt.X(xaxis_identifier, title=None, axis=alt.Axis(format=tooltip_dt_format, labelColor='dimgray')),
            y=alt.Y(count_col, title=None, axis=alt.Axis(labelColor='dimgray')),
            color=alt.condition(brush, alt.value('#83cf83'), alt.value('gray')),
            tooltip=[
                alt.Tooltip(count_col, title='Messages'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ]
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=300)
    )

    del chart_df, brush

