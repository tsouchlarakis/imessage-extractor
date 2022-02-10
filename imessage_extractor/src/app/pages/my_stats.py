import altair as alt
import datetime
import pandas as pd
import logging
import plotly.express as px
import streamlit as st
import stylecloud
from imessage_extractor.src.app.color_theme import iMessageVisualizerColors
from imessage_extractor.src.app.data.extract import iMessageDataExtract
import logging
from imessage_extractor.src.app.helpers import intword, csstext
from imessage_extractor.src.app.helpers import to_date_str
from imessage_extractor.src.helpers.utils import strip_ws
from imessage_extractor.src.helpers.verbosity import code
from nltk.corpus import stopwords
from os import remove
from os.path import dirname, join, isfile


root_dir = dirname(dirname(dirname(dirname(dirname(__file__)))))
color = iMessageVisualizerColors()


def pull_page_data(data) -> dict:
    """
    Pull necessary data for the My Stats page.
    """
    pdata = {}

    pdata['summary_day'] = data.daily_summary_vw
    pdata['summary_day_from_who'] = data.daily_summary_from_who_vw
    pdata['summary_day_contact_from_who'] = data.daily_summary_contact_from_who_vw

    pdata['summary_day']['dt'] = pd.to_datetime(pdata['summary_day']['dt'])
    pdata['summary_day_from_who']['dt'] = pd.to_datetime(pdata['summary_day_from_who']['dt'])
    pdata['summary_day_contact_from_who']['dt'] = pd.to_datetime(pdata['summary_day_contact_from_who']['dt'])

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

    pdata['summary_day_contact_from_who'] = (
        pdata['summary_day_contact_from_who']
        .loc[
            (pdata['summary_day_contact_from_who']['dt'] >= pd.to_datetime(filter_start_dt))
            & (pdata['summary_day_contact_from_who']['dt'] <= pd.to_datetime(filter_stop_dt))
        ]
    )

    if len(pdata['summary_day_contact_from_who']) == 0:
        raise ValueError('Dataframe `summary_day_contact_from_who` has no records, there must be a mistake!')

    return pdata


def resample_page_data(pdata: dict, dt_gran: str) -> pd.DataFrame:
    """
    Resample page dataframes based on selected date granularity.
    """
    assert dt_gran in ['Day', 'Week', 'Month', 'Year'], f'Invalid date granularity: {dt_gran}'

    if dt_gran == 'Day':
        pdata['summary_resample'] = pdata['summary_day']
        pdata['summary_from_who_resample'] = pdata['summary_day_from_who']
        return pdata

    # Must apply some resampling

    elif dt_gran == 'Week':
        resample_identifier = 'W-SUN'

    elif dt_gran == 'Month':
        resample_identifier = 'MS'

    elif dt_gran == 'Year':
        resample_identifier = 'AS'

    pdata['summary_resample'] = pdata['summary_day'].set_index('dt').resample(resample_identifier).sum().reset_index()

    pdata['summary_from_who_resample'] = (
        pdata['summary_day_from_who']
        .set_index(['is_from_me', 'dt'])
        .groupby([
            pdata['summary_day_from_who'].set_index(['is_from_me', 'dt']).index.get_level_values('is_from_me'),
            pd.Grouper(freq=resample_identifier, level=1),
        ])
        .sum()
        .reset_index()
    )

    pdata['summary_contact_from_who_resample'] = (
        pdata['summary_day_contact_from_who']
        .set_index(['contact_name', 'is_from_me', 'dt'])
        .groupby([
            pdata['summary_day_contact_from_who'].set_index(['contact_name', 'is_from_me', 'dt']).index.get_level_values('contact_name'),
            pdata['summary_day_contact_from_who'].set_index(['contact_name', 'is_from_me', 'dt']).index.get_level_values('is_from_me'),
            pd.Grouper(freq=resample_identifier, level=2),
        ])
        .sum()
        .reset_index()
    )

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
        tooltip_dt_title = 'Date'
        tooltip_dt_format = '%b %-d, %y'
        xaxis_identifier = 'dt:T'
        dt_offset = pd.DateOffset(days=1)

    elif dt_gran == 'Week':
        tooltip_dt_title = 'Week of'
        tooltip_dt_format = '%b %-d, %y'
        xaxis_identifier = 'dt:T'
        dt_offset = pd.DateOffset(weeks=1)

    elif dt_gran == 'Month':
        tooltip_dt_title = 'Month'
        tooltip_dt_format = '%b %y'
        xaxis_identifier = 'yearmonth(dt):O'
        dt_offset = pd.DateOffset(months=1)

    elif dt_gran == 'Year':
        tooltip_dt_title = 'Year'
        tooltip_dt_format = '%Y'
        xaxis_identifier = 'year(dt):O'
        dt_offset = pd.DateOffset(years=1)

    return tooltip_dt_title, tooltip_dt_format, xaxis_identifier, dt_offset


def get_corner_radius_size(xaxis_length: int) -> float:
    """
    Determine the optimal corner radius size dependent on the number of x-axis ticks.
    """
    if xaxis_length <= 5:
        return 5
    elif xaxis_length > 5 and xaxis_length <= 50:
        return 3
    elif xaxis_length > 50 and xaxis_length <= 100:
        return 2
    else:
        return 1


def write(data: 'iMessageDataExtract', logger: logging.Logger) -> None:
    """
    Write the My Stats page.
    """
    logger.info(f'Writing page {code("My Stats")}', bold=True)
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
    pdata['summary_day_contact_from_who'][count_col] = pdata['summary_day_contact_from_who'][selected_include_type_columns].sum(axis=1)

    # Add a dataframe called 'summary_resample' to page data dictionary based on selected date granularity
    pdata = resample_page_data(pdata, dt_gran)

    # Clean up variables not used in the rest of the function
    del dt_options, use_exact_dates, selected_include_type_columns

    #
    # Message volume
    #

    st.markdown(f'First message on **{to_date_str(first_message_dt)}**, latest message on **{to_date_str(last_message_dt)}**.')

    st.markdown(csstext('Message Volume', cls='medium-text-bold', header=True), unsafe_allow_html=True)

    # Large text reflecting message volume
    total_messages_from_me_pct = pdata['summary_day_from_who'].loc[pdata['summary_day_from_who']['is_from_me'] == 1][count_col].sum() \
        / pdata['summary_day_from_who'][count_col].sum()
    total_messages_from_others_pct = pdata['summary_day_from_who'].loc[pdata['summary_day_from_who']['is_from_me'] == 0][count_col].sum() \
        / pdata['summary_day_from_who'][count_col].sum()
    st.markdown(f"""Total messages exchanged with all contacts (including group chats),
    **{str(int(round(total_messages_from_me_pct * 100, 0))) + '%'}** sent by me,
    **{str(int(round(total_messages_from_others_pct * 100, 0))) + '%'}** sent by others.
    """)

    total_messages = pdata['summary_day'][count_col].sum()
    st.markdown(csstext(intword(total_messages), cls='large-text-green-center'), unsafe_allow_html=True)

    del total_messages, total_messages_from_me_pct, total_messages_from_others_pct

    # Plot of message volume over time
    tooltip_dt_title, tooltip_dt_format, xaxis_identifier, dt_offset = get_altair_dt_plot_attributes(dt_gran)
    st.markdown(f"Here's my total message volume I've sent and received over time, shown by **{dt_gran}**:")

    df_message_volume = pdata['summary_resample'].copy()
    df_message_volume['dt'] = df_message_volume['dt'] + dt_offset
    brush = alt.selection_interval(encodings=['x'])

    st.altair_chart(
        alt.Chart(data=df_message_volume.sort_values('dt'), background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_message_volume)),
            cornerRadiusTopRight=get_corner_radius_size(len(df_message_volume))
        ).encode(
            x=alt.X(xaxis_identifier, title=None, axis=alt.Axis(format=tooltip_dt_format, labelColor=color.xaxis_label)),
            y=alt.Y(count_col, title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
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

    del df_message_volume, brush

    #
    # Average messages by day of week
    #

    message_volume_dow = pdata['summary_day'][['dt', count_col]].reset_index().copy()
    message_volume_dow['weekday'] = message_volume_dow['dt'].dt.day_name()
    message_volume_dow = message_volume_dow.drop('dt', axis=1).groupby('weekday').mean()
    most_popular_day = message_volume_dow[count_col].idxmax()
    n_messages_on_most_popular_day = message_volume_dow[count_col].max()

    st.markdown("Here's the average number of messages I've sent and received per day of the week:")

    st.altair_chart(
        alt.Chart(data=message_volume_dow.reset_index(), background=color.background_main)
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
                    axis=alt.Axis(labelColor=color.xaxis_label)),
            y=alt.Y(count_col, title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            tooltip=[
                alt.Tooltip(count_col, title='Average messages', format='.1f'),
                alt.Tooltip('weekday', title='Weekday'),
            ]
        )
        .configure_axis(grid=False)
        .configure_axisX(labelAngle=0)
        .configure_view(strokeOpacity=0)
        .properties(width=600, height=150)
    )

    st.markdown(f'Looks like **{most_popular_day}** is my most active texting day, with an average of **{round(n_messages_on_most_popular_day, 1)}** messages exchanged that day.')

    del message_volume_dow, most_popular_day, n_messages_on_most_popular_day

    #
    # Words
    #

    st.markdown(csstext('Words', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown('A visual representation of the words used across all conversations.')

    total_tokens = pdata['summary_day']['tokens'].sum()
    total_tokens_from_me_pct = pdata['summary_day_from_who'].loc[pdata['summary_day_from_who']['is_from_me'] == 1]['tokens'].sum() / pdata['summary_day']['tokens'].sum()
    total_tokens_from_others_pct = pdata['summary_day_from_who'].loc[pdata['summary_day_from_who']['is_from_me'] == 0]['tokens'].sum() / pdata['summary_day']['tokens'].sum()

    st.markdown(csstext(intword(total_tokens), cls='large-text-green-center'), unsafe_allow_html=True)

    st.markdown(f"""Total words exchanged with all contacts,
    **{str(int(round(total_tokens_from_others_pct * 100, 0))) + '%'}** written by them,
    **{str(int(round(total_tokens_from_me_pct * 100, 0))) + '%'}** written by me.
    """)

    #
    # Wordcloud
    #

    # Pull data
    df_word_counts = (
        data.contact_token_usage_from_who_vw
        .loc[data.contact_token_usage_from_who_vw['is_from_me'] == 1]
        [['token', 'usages']]
        .groupby('token')
        .sum()
        .sort_values('usages', ascending=False)
        .reset_index()
    )

    # Filter stopwords and punctuation
    df_word_counts['token'] = df_word_counts['token'].str.lower()
    df_word_counts['token'] = (
        df_word_counts['token']
        .str.replace(r'[^\w\s]+', '', regex=True)
        .str.replace(r'^(s|d)$', '', regex=True)
    )
    df_word_counts = df_word_counts.loc[df_word_counts['token'] > '']

    expected_stylecloud_fpath = 'stylecloud_my_stats.png'

    tmp_wordcloud_fpath = 'tmp_imessage_extractor_app_pick_a_contact_wordcloud.csv'
    (
        df_word_counts
        [['token', 'usages']]
        .to_csv(tmp_wordcloud_fpath, index=False)
    )

    icon_name = 'fas fa-comment'

    stylecloud.gen_stylecloud(file_path=tmp_wordcloud_fpath,
                              icon_name=icon_name,
                              colors=[color.imessage_green, '#a6e0a6', '#dcecdc', '#ffffff'],
                              background_color=color.background_main,
                              output_name=expected_stylecloud_fpath,
                              gradient='horizontal',
                              max_words=500,
                              stopwords=True,
                              custom_stopwords=data.lst_contractions_wo_apostrophe + stopwords.words('english'),
                              size=(1024, 800),)

    st.markdown("Here's a wordcloud of the most common words (excluding stopwords) used by me:")

    if isfile(expected_stylecloud_fpath):
        st.image(expected_stylecloud_fpath)
        remove(expected_stylecloud_fpath)
    else:
        st.markdown(csstext('Wordcloud not available 😔', cls='medium-text-center'), unsafe_allow_html=True)

    if isfile(tmp_wordcloud_fpath):
        remove(tmp_wordcloud_fpath)

    #
    # Special Messages
    #

    st.markdown(csstext('Special Messages', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown('Breakdown of special messages types (threads, attachments, URLs, Apps for iMessage and tapbacks.')

    #
    # Threads
    #

    st.markdown(csstext('Threads', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(f"""Volume of
    <b><font color="{color.imessage_purple}">thread origins</font></b>,
    messages that started a new thread, and
    <b><font color="{color.imessage_green}">thread replies</font></b>,
    messages sent within threads that have already been created.
    """, unsafe_allow_html=True)

    df_threads = (
        pdata['summary_from_who_resample']
        .loc[pdata['summary_from_who_resample']['is_from_me'] == 1]
        [['dt', 'thread_origins', 'threaded_replies']]
    )

    df_threads['dt'] = df_threads['dt'] + dt_offset
    df_threads = df_threads.melt(id_vars='dt', value_vars=['thread_origins', 'threaded_replies'], var_name='Thread Type', value_name='count')
    df_threads['Thread Type'] = df_threads['Thread Type'].map({'thread_origins': 'Origins', 'threaded_replies': 'Replies'})

    st.altair_chart(
        alt.Chart(data=df_threads.sort_values('dt'), background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_threads)),
            cornerRadiusTopRight=get_corner_radius_size(len(df_threads)),
        ).encode(
            x=alt.X(xaxis_identifier, title=None, axis=alt.Axis(format=tooltip_dt_format, labelColor=color.xaxis_label)),
            y=alt.Y('count', title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            color=alt.Color(
                'Thread Type',
                scale=alt.Scale(domain=['Origins', 'Replies'], range=[color.imessage_purple, color.imessage_green]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip('count', title='Messages'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ],
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .properties(width=600, height=200)
    )

    del df_threads

    #
    # Apps for iMessage
    #

    st.markdown(csstext('Apps for iMessage', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown("""Includes <b>Fitness</b> notifications, <b>Game Pigeon</b> plays, <b>Apple Cash</b> sends and
    requests, <b>polls</b>, <b>stickers</b> and other apps for iMessage""", unsafe_allow_html=True)

    df_apps_for_imessage = pdata['summary_from_who_resample'].loc[pdata['summary_from_who_resample']['is_from_me'] == 1][['dt', 'app_for_imessage']]
    df_apps_for_imessage['dt'] = df_apps_for_imessage['dt'] + dt_offset

    brush = alt.selection_interval(encodings=['x'])

    st.altair_chart(
        alt.Chart(data=df_apps_for_imessage.sort_values('dt'), background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_apps_for_imessage)),
            cornerRadiusTopRight=get_corner_radius_size(len(df_apps_for_imessage))
        ).encode(
            x=alt.X(xaxis_identifier, title=None, axis=alt.Axis(format=tooltip_dt_format, labelColor=color.xaxis_label)),
            y=alt.Y('app_for_imessage', title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
            tooltip=[
                alt.Tooltip('app_for_imessage', title='App Messages'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ],
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=200)
    )

    del df_apps_for_imessage, brush

    #
    # Attachments
    #

    st.markdown(csstext('Attachments', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(f"""Volume of
    <b><font color="{color.imessage_purple}">text messages with attachments</font></b>, and
    <b><font color="{color.imessage_green}">attachments sent by themselves</font></b>.
    """, unsafe_allow_html=True)

    df_attachments = pdata['summary_from_who_resample'].loc[pdata['summary_from_who_resample']['is_from_me'] == 1][['dt', 'messages_containing_attachments', 'messages_attachments_only']]
    df_attachments['dt'] = df_attachments['dt'] + dt_offset
    df_attachments = df_attachments.melt(id_vars='dt', value_vars=['messages_containing_attachments', 'messages_attachments_only'], var_name='Attachment Type', value_name='count')
    df_attachments['Attachment Type'] = df_attachments['Attachment Type'].map({'messages_containing_attachments': 'Messages with Attachments', 'messages_attachments_only': 'Attachments Only'})

    st.altair_chart(
        alt.Chart(data=df_attachments.sort_values('dt'), background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_attachments)),
            cornerRadiusTopRight=get_corner_radius_size(len(df_attachments))
        ).encode(
            x=alt.X(xaxis_identifier, title=None, axis=alt.Axis(format=tooltip_dt_format, labelColor=color.xaxis_label)),
            y=alt.Y('count', title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            color=alt.Color(
                'Attachment Type',
                scale=alt.Scale(domain=['Messages with Attachments', 'Attachments Only'], range=[color.imessage_purple, color.imessage_green]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip('count', title='Attachments'),
                alt.Tooltip('dt', title=tooltip_dt_title, format=tooltip_dt_format),
            ],
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .properties(width=600, height=200)
    )

    del df_attachments

    #
    # Tapbacks
    #

    st.markdown(csstext('Tapbbacks', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown("How frequently I use each flavor of tapback.")

    df_tapback = (
        data.message_user
        .reset_index()
        .loc[
            (data.message_user.reset_index()['is_from_me'] == 1)
            & (data.message_user.reset_index()['is_emote'] == 1)
        ]
        .groupby(['is_from_me', 'message_special_type'])
        .agg({'message_id': 'count'})
        .reset_index()
        .rename(columns={'message_id': 'count', 'message_special_type': 'tapback'})
    )
    df_tapback['tapback'] = df_tapback['tapback'].replace(data.map_tapback_type)

    all_tapbacks = list(data.map_tapback_type.values())

    df_tapback = (
        df_tapback
        .loc[df_tapback['is_from_me'] == True]
        .drop('is_from_me', axis=1)
        .merge(pd.DataFrame(all_tapbacks, columns=['tapback']), how='outer', on='tapback')
        .fillna(0.0)
    )

    # Color ramp generator: https://www.geeksforgeeks.org/pie-plot-using-plotly-in-python/
    color_map = {
        'Love': '#cde7d2',
        'Like': '#aedfb2',
        'Dislike': '#8fd792',
        'Laugh': '#70cf72',
        'Emphasis': '#51c752',
        'Question': '#32c032',
        'Remove Heart': color.imessage_purple,
        'Remove Like': '#d6bae8',
        'Remove Dislike': '#cda9e2',
        'Remove Laugh': '#c498dc',
        'Remove Emphasis': '#bb87d6',
        'Remove Question': '#b377d0',
    }

    fig = px.pie(
        df_tapback,
        hole=.5,
        values='count',
        names='tapback',
        color_discrete_sequence=list(color_map.values()),
        width=640,
        height=570,
    )

    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='Tapback: <b>%{label}</b><br>Usages: <b>%{value}</b>',
    )

    fig.update_layout(
        showlegend=False,
    )

    st.plotly_chart(fig)

    #
    # iMessage vs. SMS
    #

    st.markdown(csstext('iMessage 🥳 v. SMS 😡', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown("Breakdown of iMessage (text, tapbacks, etc.) and SMS (just text) usage, and offenders who resist switching to iMessage.")

    df_imessage_vs_sms = (
        data.summary_vw
        [['imessages', 'sms']]
        .T
        .reset_index()
        .rename(columns={'index': 'Message Medium', 0: 'Count'})
    )

    df_imessage_vs_sms['Message Medium'] = df_imessage_vs_sms['Message Medium'].map({'imessages': 'iMessage', 'sms': 'SMS'})

    st.altair_chart(
        alt.Chart(data=df_imessage_vs_sms, background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_imessage_vs_sms)),
            cornerRadiusTopRight=get_corner_radius_size(len(df_imessage_vs_sms)),
        ).encode(
            x=alt.X('Message Medium', title=None, sort=['iMessage', 'SMS'], axis=alt.Axis(labelColor=color.xaxis_label, labelAngle=0)),
            y=alt.Y('Count', title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            color=alt.Color(
                'Message Medium',
                scale=alt.Scale(domain=['iMessage', 'SMS'], range=[color.imessage_blue, color.imessage_green]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip('Message Medium', title='Medium'),
                alt.Tooltip('Count', title='Usages'),
            ],
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .properties(width=600, height=300)
    )

    del df_imessage_vs_sms

    st.markdown(csstext('iMessage Offenders', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown('Pressure these fools into joining the coveted Ecosystem.')

    df_imessage_offenders = (
        data
        .summary_contact_from_who_vw[['contact_name', 'sms']]
        .groupby('contact_name')
        .sum()  # Sum is_from_me = 0 and is_from_me = 1
        .sort_values('sms', ascending=False)
        .merge(data.contact_group_chat_map_vw, how='left', on='contact_name')
    )

    df_imessage_offenders = df_imessage_offenders.loc[
        (df_imessage_offenders['sms'] > 0)
        & (df_imessage_offenders['is_group_chat'] == 0)
    ]

    col1, col2 = st.columns((1, 2.5))
    n_rows_display = col1.number_input('', min_value=1, max_value=len(df_imessage_offenders), value=min(20, len(df_imessage_offenders)), step=1)


    brush = alt.selection_interval(encodings=['y'])
    sort_order = df_imessage_offenders['contact_name'].tolist()

    st.altair_chart(
        alt.Chart(data=df_imessage_offenders.head(n_rows_display), background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_imessage_offenders.head(n_rows_display))),
            cornerRadiusTopRight=get_corner_radius_size(len(df_imessage_offenders.head(n_rows_display))),
        ).encode(
            x=alt.X('sms', title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            y=alt.Y('contact_name', title=None, sort=sort_order, axis=alt.Axis(labelColor=color.xaxis_label, labelAngle=0)),
            color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
            tooltip=[
                alt.Tooltip('contact_name', title='Contact'),
                alt.Tooltip('sms', title='🤢 SMS Messages'),
            ],
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=500)
    )

    del brush, df_imessage_offenders, sort_order, col1, col2, n_rows_display

    #
    # People
    #

    st.markdown(csstext('People', cls='medium-text-bold', header=True), unsafe_allow_html=True)

    st.markdown(csstext('Favorite Contacts', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown('Who do I message the most? Includes both sent and received messages to individual contacts (excluding group chats).')

    df_favorite_contacts = (
        pdata['summary_contact_from_who_resample']
        .groupby('contact_name')
        .sum()
        .reset_index()
        [['contact_name', count_col]]
        .sort_values(count_col, ascending=False)
        .merge(data.contact_group_chat_map_vw, how='left', on='contact_name')
    )

    df_favorite_contacts = (
        df_favorite_contacts
        .loc[df_favorite_contacts['is_group_chat'] == 0]
    )

    col1, col2 = st.columns((1, 2.5))
    n_rows_display = col1.number_input('', min_value=1, max_value=len(df_favorite_contacts), value=min(20, len(df_favorite_contacts)), step=1)

    brush = alt.selection_interval(encodings=['y'])
    sort_order = df_favorite_contacts['contact_name'].tolist()

    st.altair_chart(
        alt.Chart(data=df_favorite_contacts.head(n_rows_display), background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_favorite_contacts.head(n_rows_display))),
            cornerRadiusTopRight=get_corner_radius_size(len(df_favorite_contacts.head(n_rows_display))),
        ).encode(
            x=alt.X(count_col, title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            y=alt.Y('contact_name', title=None, sort=sort_order, axis=alt.Axis(labelColor=color.xaxis_label, labelAngle=0)),
            color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
            tooltip=[
                alt.Tooltip('contact_name', title='Contact'),
                alt.Tooltip(count_col, title='Messages'),
            ],
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=500)
    )

    del brush, df_favorite_contacts, sort_order, col1, col2, n_rows_display

    st.markdown(csstext('Favorite Group Chats', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown('Which group chats do I message the most? Includes both sent and received messages.')

    df_favorite_group_chats = (
        pdata['summary_contact_from_who_resample']
        .groupby('contact_name')
        .sum()
        .reset_index()
        [['contact_name', count_col]]
        .sort_values(count_col, ascending=False)
        .merge(data.contact_group_chat_map_vw, how='left', on='contact_name')
    )

    df_favorite_group_chats = (
        df_favorite_group_chats
        .loc[df_favorite_group_chats['is_group_chat'] == 1]
    )

    col1, col2 = st.columns((1, 2.5))
    n_rows_display = col1.number_input('', min_value=1, max_value=len(df_favorite_group_chats), value=min(20, len(df_favorite_group_chats)), step=1)

    brush = alt.selection_interval(encodings=['y'])
    sort_order = df_favorite_group_chats['contact_name'].tolist()

    st.altair_chart(
        alt.Chart(data=df_favorite_group_chats.head(n_rows_display), background=color.background_main)
        .mark_bar(
            cornerRadiusTopLeft=get_corner_radius_size(len(df_favorite_group_chats.head(n_rows_display))),
            cornerRadiusTopRight=get_corner_radius_size(len(df_favorite_group_chats.head(n_rows_display))),
        ).encode(
            x=alt.X(count_col, title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
            y=alt.Y('contact_name', title=None, sort=sort_order, axis=alt.Axis(labelColor=color.xaxis_label, labelAngle=0)),
            color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
            tooltip=[
                alt.Tooltip('contact_name', title='Contact'),
                alt.Tooltip(count_col, title='Messages'),
            ],
        )
        .configure_axis(grid=False)
        .configure_view(strokeOpacity=0)
        .add_selection(brush)
        .properties(width=600, height=500)
    )

    del brush, df_favorite_group_chats, sort_order, col1, col2, n_rows_display

    #
    # Message Snapshot
    #

    # st.markdown(csstext('Recent Snapshot', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    # st.markdown('My latest messages sent and received.')

    # col1, col2 = st.columns((1, 2.5))

    # df_search_messages = (
    #     data.message_user
    #     .loc[(data.message_user['is_text'])
    #          & (data.message_user['is_empty'] == 0)
    #         #  & (data.message_user['contact_name'] == contact_name)
    #          & (data.message_user['dt'] >= pd.to_datetime(filter_start_dt))
    #             & (data.message_user['dt'] <= pd.to_datetime(filter_stop_dt))]
    #     [['ts', 'contact_name', 'text', 'is_from_me']]
    #     .rename(columns={'contact_name': 'Contact Name', 'text': 'Text', 'is_from_me': 'From Me'})
    #     .sort_values('ts', ascending=False)
    # )
    # df_search_messages['From Me'] = df_search_messages['From Me'].map({1: 'Yes', 0: 'No'})
    # df_search_messages['Time'] = df_search_messages['ts'].dt.strftime("%b %-d '%y at %I:%M:%S %p").str.lower().str.capitalize()

    # n_message_display = col1.number_input('Show this many messages',
    #                                       min_value=1,
    #                                       max_value=len(df_search_messages),
    #                                       value=min(20, len(df_search_messages)),
    #                                       step=1)

    # st.write(df_search_messages[['Time', 'Contact Name', 'From Me', 'Text']].set_index('Time').fillna('').head(n_message_display))

    #
    # Wrap Up
    #

    logger.info('Done', arrow='black')
