import pandas as pd
import streamlit as st
import datetime
from imessage_extractor.src.helpers.utils import strip_ws
from imessage_extractor.src.helpers.verbosity import bold


def controls(data) -> tuple:
    """
    Prepare controls for Pick a Contact page.
    """
    show_group_chats = st.checkbox('Show group chats', value=False, help="Include group chat names in the 'Contact name' dropdown?")

    col1, col2 = st.columns(2)

    contact_names_display = data.lst_contact_names_all if show_group_chats else data.lst_contact_names_no_group_chats
    assert len(contact_names_display), 'No contact names in contact.csv or chat identifiers. Something is catastrophically wrong.'

    contact_name = col1.selectbox(
        label='Contact name',
        options=contact_names_display,
        index=contact_names_display[0],
        # index=contact_names_display.index('Maria Sooklaris'),
        help="Choose a contact you'd like to analyze data for!"
    )

    dt_options = ['Day', 'Week', 'Month', 'Year']
    dt_gran = col2.selectbox(
        label='Date granularity',
        options=dt_options,
        index=dt_options.index('Month'),
        help='Determine the date granularity of the visualizations below'
    )

    return contact_name, dt_gran, show_group_chats


def pull_contact_summary(data, contact_name: str, include_is_from_me: bool) -> pd.DataFrame:
    """
    Subset the daily summary dataset for the selected contact. Return a dataframe
    aggregated at the day level.
    """
    if include_is_from_me:
        df = (
            data.daily_summary_contact_from_who_vw
            .loc[data.daily_summary_contact_from_who_vw.index.get_level_values('contact_name') == contact_name]
            .droplevel('contact_name')
        )

        df = df.reset_index().pivot(index='dt', columns='is_from_me', values=[x for x in df.columns])
        df.columns = ['_'.join([str(x) for x in pair]) for pair in df.columns]
        df.columns = [x.replace('1', 'from_me').replace('0', 'from_them') for x in df.columns]

    else:
        df = (
            data.daily_summary_contact_vw
            .loc[data.daily_summary_contact_vw.index.get_level_values('contact_name') == contact_name]
            .droplevel('contact_name')
        )

        df.index = pd.to_datetime(df.index)

    return df


def prepare_page_data(data, contact_name: str, dt_gran: str) -> tuple:
    """
    Query and prepare data for Pick a Contact page.
    """
    # Initialize a dictionary to store user inputs used to write this page
    inputs = {}

    # Initialize a ditionary to store datasets used to write this page
    page_data = {}

    # Initialize dictionary to store high-level statistics (usually a single number or date)
    # to be written on this page
    stats = {}

    page_data['summary_day'] = pull_contact_summary(data, contact_name, include_is_from_me=False)
    page_data['summary_day_from_who'] = pull_contact_summary(data, contact_name, include_is_from_me=True)

    # Get max and min message dates for this contact
    stats['first_message_dt'] = page_data['summary_day'].index.get_level_values('dt').min()
    stats['last_message_dt'] = page_data['summary_day'].index.get_level_values('dt').max()

    # Add filters now that first and last message dates are known
    use_exact_dates = st.checkbox(
        label='Use exact dates',
        value=False,
        help="Show date filter as a relative date dropdown select list, instead of specific start/stop date inputs?"
    )

    col1, col2 = st.columns(2)

    if use_exact_dates:
        inputs['filter_start_dt'] = col1.date_input(
            label='Start date',
            value=stats['first_message_dt'],
            help='''Set the beginning of the date range to display data for. Defaults to the
            earliest date that at least one message was exchanged with this contact.'''
        )
        inputs['filter_stop_dt'] = col2.date_input(
            label='End date',
            value=stats['last_message_dt'],
            help='''Set the end of the date range to display data for. Defaults to the
            last date that at least one message was exchanged with this contact.'''
        )

    else:
        inputs['filter_stop_dt'] = datetime.date.today()

        relative_date_map = {
            'Today': datetime.date.today() - datetime.timedelta(days=0),
            'This week': datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday()) - datetime.timedelta(days=1),
            'This month': datetime.date.today().replace(day=1),
            'This year': datetime.datetime.now().date().replace(month=1, day=1),
            'Last 60 days': datetime.date.today() - datetime.timedelta(days=60),
            'Last 90 days': datetime.date.today() - datetime.timedelta(days=90),
            'Last 6 months': datetime.date.today() - datetime.timedelta(days=6*30),
            'Last 12 months': datetime.date.today() - datetime.timedelta(days=365),
            'All time': stats['first_message_dt'],
        }

        selected_stop_dt = st.selectbox(
            label='Date filter',
            options=relative_date_map.keys(),
            index=list(relative_date_map.keys()).index('All time'),
            help='Determine the date granularity of the visualizations below'
        )

        inputs['filter_start_dt'] = relative_date_map[selected_stop_dt]


    include_types_options = ['Text', 'Emote', 'Attachment', 'URL', 'App for iMessage']
    include_types_columns = ['text_messages', 'emotes', 'messages_attachment_only', 'urls', 'app_for_imessage']
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

    # Appply date filter
    page_data['summary_day'] = (
        page_data['summary_day']
        .loc[
            (page_data['summary_day'].index >= pd.to_datetime(inputs['filter_start_dt']))
            & (page_data['summary_day'].index <= pd.to_datetime(inputs['filter_stop_dt']))
        ]
    )

    if len(page_data['summary_day']) == 0:
        raise ValueError('Dataframe `summary_day` has no records, there must be a mistake!')


    def check_message_column_sums_align(df: pd.DataFrame, include_types_columns: list) -> None:
        """
        Make sure that the sum of all message type counts is equal to the 'messages' column
        in the daily summary. If these counts do not align, then ther emay be one or more
        messages that were not captured by the message type filters.
        """
        df1 = df['messages'].rename('messages_truth').reset_index()
        df2 = df[include_types_columns].sum(axis=1).rename('messages_challenger').reset_index()
        df3 = df1.merge(df2, on='dt')
        df3['unequal'] = df3['messages_truth'] != df3['messages_challenger']

        if len(df3[df3['unequal']]) > 0:
            raise Exception(f"""Overall message count in daily summary 'messages' column
            does not align with the sum of the include message types defined in pick_a_contact.py.
            It might be that there are one or more messages in the data that were not captured
            by these include message type filters. We'll need to check the aggregation logic
            in daily_summary_contact_from_who_vw.sql to make sure that the sum of each column
            corresponding to the include message types matches the count of all `message_id` values.
            This problem was found with contact {bold(contact_name)} on date(s)
            {str(list(df3[df3['unequal']]['dt']))}""")


    check_message_column_sums_align(page_data['summary_day'], include_types_columns)

    # Apply message type filter
    selected_include_type_columns = []
    if 'Text' in include_types:
        selected_include_type_columns.append('text_messages')
    if 'Emote' in include_types:
        selected_include_type_columns.append('emotes')
    if 'Attachment' in include_types:
        selected_include_type_columns.append('messages_attachment_only')
    if 'URL' in include_types:
        selected_include_type_columns.append('urls')
    if 'App for iMessage' in include_types:
        # Includes workout notifications and Apple Cash
        selected_include_type_columns.append('app_for_imessage')


    message_count_col = 'pick_a_contact_display_message_count'
    page_data['summary_day'][message_count_col] = page_data['summary_day'][selected_include_type_columns].sum(axis=1)

    # Days with at least one message exchanged for this contact in filter range
    stats['active_texting_days'] = page_data['summary_day'].loc[page_data['summary_day'][message_count_col] > 0].shape[0]

    # Days with at least one message exchanged for any contact in filter range
    stats['total_active_texting_days'] = len(
        data.message_user.loc[(data.message_user['dt'] >= pd.to_datetime(inputs['filter_start_dt']))
        & (data.message_user['dt'] <= pd.to_datetime(inputs['filter_stop_dt']))]['dt'].unique()
    )

    # Percent of total active days across all contacts with at least one message exchanged
    # to or from the selected contact
    stats['active_texting_days_pct'] = round(stats['active_texting_days'] / stats['total_active_texting_days'] * 100, 1)

    # Resample to selected date granularity
    page_data['summary'] = resample_dataframe(page_data['summary_day'], dt_gran)

    return inputs, page_data, stats, message_count_col, selected_include_type_columns


def resample_dataframe(df: pd.DataFrame, dt_gran: str) -> pd.DataFrame:
    """
    Resample summary dataframe based on selected date granularity.
    """
    assert dt_gran in ['Day', 'Week', 'Month', 'Year'], f'Invalid date granularity: {dt_gran}'

    df.index = pd.to_datetime(df.index)

    if dt_gran == 'Day':
        return df
    elif dt_gran == 'Week':
        return df.resample('W-SUN').sum()
    elif dt_gran == 'Month':
        return df.resample('MS').sum()
    elif dt_gran == 'Year':
        return df.resample('AS').sum()


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
