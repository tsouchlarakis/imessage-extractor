from os import remove
from nltk.corpus import stopwords
from os.path import isfile, join, dirname
import altair as alt
import datetime
import stylecloud
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from imessage_extractor.src.app.helpers import to_date_str, intword, csstext, htmlbold
from imessage_extractor.src.helpers.utils import strip_ws
from imessage_extractor.src.helpers.verbosity import bold


root_dir = dirname(dirname(dirname(dirname(dirname(__file__)))))


def write(data, logger) -> None:
    """
    Write the Pick a Contact page.
    """
    logger.info('Writing page "Pick a Contact"')
    st.image(join(root_dir, 'graphics', 'pick_a_contact.png'))

    # Controls
    show_group_chats = st.checkbox('Show group chats', value=False, help="Include group chat names in the 'Contact name' dropdown?")

    col1, col2 = st.columns(2)

    contact_names_display = data.lst_contact_names_all if show_group_chats else data.lst_contact_names_no_group_chats
    contact_name = col1.selectbox(
        label='Contact name',
        options=contact_names_display,
        index=contact_names_display.index('Maria Sooklaris'),
        help="Choose a contact you'd like to analyze data for!"
    )

    dt_options = ['Day', 'Week', 'Month', 'Year']
    dt_gran = col2.selectbox(
        label='Date granularity',
        options=dt_options,
        index=dt_options.index('Month'),
        help='Determine the date granularity of the visualizations below'
    )


    def pull_contact_summary(contact_name: str, include_is_from_me: bool=False) -> pd.DataFrame:
        """
        Subset the daily summary dataset for the selected contact. Return a dataframe
        aggregated at the day level.
        """
        if include_is_from_me:
            df = (
                data.daily_summary_contact_from_who
                .loc[data.daily_summary_contact_from_who.index.get_level_values('contact_name') == contact_name]
                .droplevel('contact_name')
            )

            df = df.reset_index().pivot(index='dt', columns='is_from_me', values=[x for x in df.columns])
            df.columns = ['_'.join([str(x) for x in pair]) for pair in df.columns]
            df.columns = [x.replace('True', 'from_me').replace('False', 'from_them') for x in df.columns]
        else:
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


    # Initialize a dictionary to store user inputs used to write this page
    inputs = {}

    # Initialize a ditionary to store datasets used to write this page
    page_data = {}

    # Initialize dictionary to store high-level statistics (usually a single number or date)
    # to be written on this page
    stats = {}

    page_data['summary_day'] = pull_contact_summary(contact_name)
    page_data['summary_day_from_who'] = pull_contact_summary(contact_name, include_is_from_me=True)

    # Get max and min message dates for this contact
    stats['first_message_dt'] = page_data['summary_day'].index.get_level_values('dt').min()
    stats['last_message_dt'] = page_data['summary_day'].index.get_level_values('dt').max()

    # Add filters now that first and last message dates are known
    use_exact_dates = st.checkbox(
        label='Use exact dates',
        value=False,
        help="Show date filter as a relative date dropdown select list, instead of specific start/stop date inputs?"
    )

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
            in daily_summary_contact_from_who.sql to make sure that the sum of each column
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
        selected_include_type_columns.append('messages_attachments_only')
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

    st.markdown(csstext(f'''
    First message on {htmlbold(to_date_str(stats["first_message_dt"]))},
    latest message on {htmlbold(to_date_str(stats["last_message_dt"]))}
    ''', cls='small-text'), unsafe_allow_html=True)

    st.markdown(csstext('Active Texting Days', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(csstext('At least one message exchanged on...', cls='small-text'), unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    col1.markdown(csstext(stats['active_texting_days'], cls='large-text-green'), unsafe_allow_html=True)
    col2.markdown(csstext(str(stats['active_texting_days_pct']) + '%', cls='large-text-green'), unsafe_allow_html=True)
    col1.markdown(csstext('active texting days', cls='small-text'), unsafe_allow_html=True)
    col2.markdown(csstext('of days in selected time window', cls='small-text'), unsafe_allow_html=True)

    top_active_days = page_data['summary_day'][message_count_col].sort_values(ascending=False).head(10).reset_index().reset_index()

    st.markdown('<br>', unsafe_allow_html=True)
    st.markdown(csstext('Here are the top 10 days with our highest message volume (a larger circle means more messages were exchanged on that day):', cls='small-text'), unsafe_allow_html=True)

    st.altair_chart(
        alt.Chart(top_active_days.reset_index())
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

    st.markdown('<br>', unsafe_allow_html=True)

    #
    # Message volume
    #

    st.markdown(csstext('Message Volume', cls='medium-text-bold', header=True), unsafe_allow_html=True)

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

    st.markdown(csstext(f'Looks like {htmlbold(most_popular_day)} is our most popular texting day!', cls='small-text'), unsafe_allow_html=True)


    st.markdown(csstext('% of All My Messages', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(csstext(f'Percent of my total volume (across all contacts) made up by {htmlbold(contact_name)}:', cls='small-text'), unsafe_allow_html=True)

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


    def get_point_size(xaxis_length: int) -> float:
        """
        Apply a piecewise formula to determine the optimal point size dependent on the
        number of x-axis ticks.
        """
        if xaxis_length <= 95:
            return 242.192 - 1.91781 * xaxis_length
        elif xaxis_length > 95:
            return 67.4074 - 0.0779727 * xaxis_length


    chart_df = pct_message_volume.copy()
    chart_df.index = chart_df.reset_index()['dt'] + dt_offset
    st.altair_chart(
        alt.Chart(chart_df['rate'].sort_index().reset_index())
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

    #
    # Words
    #

    st.markdown(csstext('Words', cls='medium-text-bold', header=True), unsafe_allow_html=True)

    stats['total_tokens'] = page_data['summary_day']['tokens'].sum()
    stats['total_tokens_from_me_pct'] = page_data['summary_day_from_who']['tokens_from_me'].sum() / page_data['summary_day']['tokens'].sum()
    stats['total_tokens_from_them_pct'] = page_data['summary_day_from_who']['tokens_from_them'].sum() / page_data['summary_day']['tokens'].sum()

    st.markdown(csstext(intword(stats['total_tokens']), cls='large-text-green'), unsafe_allow_html=True)
    st.markdown(f"""I've exchanged {htmlbold(intword(stats['total_tokens']))} total
    words with {htmlbold(contact_name)},
    {htmlbold(str(int(round(stats['total_tokens_from_me_pct'] * 100, 0))) + '%')} written by me,
    {htmlbold(str(int(round(stats['total_tokens_from_them_pct'] * 100, 0))) + '%')} written by them.
    """, unsafe_allow_html=True)
    st.markdown('<br>', unsafe_allow_html=True)


    page_data['word_counts'] = (
        data.message_vw
        .loc[data.message_vw['contact_name'] == contact_name]
        [['contact_name', 'is_from_me']]
        .merge(data.message_tokens_unnest, left_on='message_id', right_on='message_id')
        .groupby('token')
        .agg({'token': 'count'})
        .rename(columns={'token': 'count'})
        .sort_values('count', ascending=False)
        .reset_index()
    )

    # Filter stopwords and punctuation
    page_data['word_counts']['token'] = page_data['word_counts']['token'].str.lower()

    page_data['word_counts']['token'] = (
        page_data['word_counts']['token']
        .str.replace(r'[^\w\s]+', '')
        .str.replace(r'^(s|d)$', '')
    )
    page_data['word_counts'] = page_data['word_counts'].loc[page_data['word_counts']['token'] > '']

    tmp_wordcloud_fpath = 'tmp_imessage_extractor_app_pick_a_contact_wordcloud.csv'
    page_data['word_counts'].head(500).to_csv(tmp_wordcloud_fpath, index=False)

    expected_stylecloud_fpath = 'stylecloud.png'
    stylecloud.gen_stylecloud(file_path=tmp_wordcloud_fpath,
                              icon_name='fas fa-comment',
                              colors=['#83cf83', '#a6e0a6', '#dcecdc', '#ffffff'],
                              background_color='#2b2b2b',
                              output_name=expected_stylecloud_fpath,
                              gradient='horizontal',
                              max_words=500,
                              stopwords=True,
                              custom_stopwords=data.contractions_wo_apostrophe + stopwords.words('english'),
                              size=(1024, 800),)

    if isfile(expected_stylecloud_fpath):
        st.image('stylecloud.png')
    else:
        st.markdown(csstext('Wordcloud not available 😔', cls='medium-text-center'), unsafe_allow_html=True)

    if isfile(tmp_wordcloud_fpath):
        remove(tmp_wordcloud_fpath)

    if isfile(expected_stylecloud_fpath):
        remove(expected_stylecloud_fpath)

    stats['avg_words_per_message_from_me'] = \
        (page_data['summary_day_from_who'].sum()['tokens_from_me']
         / page_data['summary_day_from_who'].sum()['text_messages_from_me'])

    stats['avg_words_per_message_from_them'] = \
        (page_data['summary_day_from_who'].sum()['tokens_from_them']
         / page_data['summary_day_from_who'].sum()['text_messages_from_them'])

    col1, col2 = st.columns(2)

    col1.markdown(csstext(round(stats['avg_words_per_message_from_me'], 1), cls='large-text-green'), unsafe_allow_html=True)
    col1.markdown(csstext(f'My average words per message', cls='small-text'), unsafe_allow_html=True)

    col2.markdown(csstext(round(stats['avg_words_per_message_from_them'], 1), cls='large-text-green'), unsafe_allow_html=True)
    col2.markdown(csstext(f'Their average words per message', cls='small-text'), unsafe_allow_html=True)

    st.markdown('<br>', unsafe_allow_html=True)

    # Favorite words by word length

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
            & (token_usage_from_who_ranked_by_length['length'] <= 20)
        ]
        .drop('rank', axis=1)
        .sort_values(['is_from_me', 'length'])
    )

    print(token_usage_from_who_ranked_by_length)
    print(page_data['top_token_usage_from_who_by_length'])

    #
    # Tabular message preview
    #

    st.markdown(csstext('Chat History', cls='medium-text-bold', header=True), unsafe_allow_html=True)
    st.markdown(csstext(f'Snapshot of my message history with {htmlbold(contact_name)}, most recent messages first', cls='small-text'), unsafe_allow_html=True)

    message_snapshot = (
        data.message_vw
        [['ts', 'text', 'is_from_me']]
        .loc[(data.message_vw['is_text'])
             & (~data.message_vw['is_empty'])
             & (data.message_vw['contact_name'] == contact_name)
             & (data.message_vw['dt'] >= pd.to_datetime(inputs['filter_start_dt']))
                & (data.message_vw['dt'] <= pd.to_datetime(inputs['filter_stop_dt']))]
    )

    col1, col2 = st.columns((1, 2.5))

    n_message_display = col1.number_input('Show this many messages', min_value=1, max_value=len(message_snapshot), value=20, step=1)

    message_snapshot_display = pd.concat([
        (
            message_snapshot
            .loc[message_snapshot['is_from_me']]
            .rename(columns={'text': 'Me'})
            [['ts', 'Me']]
        ),
        (
            message_snapshot
            .loc[~message_snapshot['is_from_me']]
            .rename(columns={'text': contact_name})
            [['ts', contact_name]]
        )
    ], axis=0).sort_values('ts', ascending=False)

    message_snapshot_display['Time'] = message_snapshot_display['ts'].dt.strftime("%b %-d '%y at %I:%M:%S %p").str.lower().str.capitalize()

    st.write(message_snapshot_display[['Time', contact_name, 'Me']].set_index('Time').fillna('').head(n_message_display))

    logger.info('=> done')
