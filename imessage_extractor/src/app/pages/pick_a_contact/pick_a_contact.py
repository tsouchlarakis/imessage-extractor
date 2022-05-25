import altair as alt
import datetime
import humanize
import logging
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import stylecloud
from imessage_extractor.src.app.color_theme import iMessageVisualizerColors
from imessage_extractor.src.app.data.extract import iMessageDataExtract
from imessage_extractor.src.app.helpers import intword, csstext
from imessage_extractor.src.app.helpers import to_date_str
from imessage_extractor.src.helpers.utils import strip_ws
from imessage_extractor.src.helpers.verbosity import code
from nltk.corpus import stopwords
from os import remove
from os.path import dirname, join, isfile


root_dir = dirname(dirname(dirname(dirname(dirname(dirname(__file__))))))
color = iMessageVisualizerColors()


def pull_page_data(data: 'iMessageDataExtract') -> dict:
    """
    Pull necessary data for the Pick a Contact page.
    """
    pdata = {}

    pdata['summary'] = data.summary_contact_vw
    pdata['summary_from_who'] = data.summary_contact_from_who_vw

    pdata['daily_summary'] = data.daily_summary_contact_vw  # Will get filtered for contact
    pdata['daily_summary_all_contacts'] = data.daily_summary_contact_vw  # Will not get filtered for contact
    pdata['daily_summary_from_who'] = data.daily_summary_contact_from_who_vw

    pdata['daily_summary']['dt'] = pd.to_datetime(pdata['daily_summary']['dt'])
    pdata['daily_summary_all_contacts']['dt'] = pd.to_datetime(pdata['daily_summary_all_contacts']['dt'])
    pdata['daily_summary_from_who']['dt'] = pd.to_datetime(pdata['daily_summary_from_who']['dt'])

    return pdata


def filter_page_data(pdata: dict, filter_start_dt: datetime.datetime, filter_stop_dt: datetime.datetime, contact_name: str) -> dict:
    """
    Apply controls filters to page data.
    """
    pdata['summary'] = pdata['summary'].loc[pdata['summary']['contact_name'] == contact_name].copy()
    pdata['summary_from_who'] = pdata['summary_from_who'].loc[pdata['summary_from_who']['contact_name'] == contact_name].copy()

    pdata['daily_summary'] = (
        pdata['daily_summary']
        .loc[
            (pdata['daily_summary']['contact_name'] == contact_name)
            & (pdata['daily_summary']['dt'] >= pd.to_datetime(filter_start_dt))
            & (pdata['daily_summary']['dt'] <= pd.to_datetime(filter_stop_dt))
        ]
    ).copy()

    if len(pdata['daily_summary']) == 0:
        raise ValueError('Dataframe `daily_summary` has no records, there must be a mistake! Perhaps something went wrong with filtering the dataframe?')

    pdata['daily_summary_all_contacts'] = (
        pdata['daily_summary_all_contacts']
        .loc[
            (pdata['daily_summary_all_contacts']['dt'] >= pd.to_datetime(filter_start_dt))
            & (pdata['daily_summary_all_contacts']['dt'] <= pd.to_datetime(filter_stop_dt))
        ]
    ).copy()

    pdata['daily_summary_from_who'] = (
        pdata['daily_summary_from_who']
        .loc[
            (pdata['daily_summary_from_who']['contact_name'] == contact_name)
            & (pdata['daily_summary_from_who']['dt'] >= pd.to_datetime(filter_start_dt))
            & (pdata['daily_summary_from_who']['dt'] <= pd.to_datetime(filter_stop_dt))
        ]
    ).copy()

    if len(pdata['daily_summary_from_who']) == 0:
        raise ValueError('Dataframe `daily_summary_from_who` has no records, there must be a mistake!')

    return pdata


def resample_page_data(pdata: dict, dt_gran: str) -> pd.DataFrame:
    """
    Resample page dataframes based on selected date granularity.
    """
    assert dt_gran in ['Day', 'Week', 'Month', 'Year'], f'Invalid date granularity: {dt_gran}'

    if dt_gran == 'Day':
        pdata['summary_resample'] = pdata['daily_summary']
        pdata['summary_all_contacts_resample'] = pdata['daily_summary_all_contacts']
        pdata['summary_from_who_resample'] = pdata['daily_summary_from_who']
        return pdata

    # Must apply some resampling

    elif dt_gran == 'Week':
        resample_identifier = 'W-SUN'

    elif dt_gran == 'Month':
        resample_identifier = 'MS'

    elif dt_gran == 'Year':
        resample_identifier = 'AS'

    pdata['summary_resample'] = pdata['daily_summary'].set_index('dt').resample(resample_identifier).sum().reset_index()

    pdata['summary_all_contacts_resample'] = (
        pdata['daily_summary_all_contacts']
        .set_index(['contact_name', 'dt'])
        .groupby([
            pdata['daily_summary_all_contacts'].set_index(['contact_name', 'dt']).index.get_level_values('contact_name'),
            pd.Grouper(freq=resample_identifier, level=1),
        ])
        .sum()
        .reset_index()
    )

    pdata['summary_from_who_resample'] = (
        pdata['daily_summary_from_who']
        .set_index(['is_from_me', 'dt'])
        .groupby([
            pdata['daily_summary_from_who'].set_index(['is_from_me', 'dt']).index.get_level_values('is_from_me'),
            pd.Grouper(freq=resample_identifier, level=1),
        ])
        .sum()
        .reset_index()
    )

    return pdata


def get_point_size(xaxis_length: int) -> float:
    """
    Apply a piecewise formula to determine the optimal point size dependent on the
    number of x-axis ticks.
    """
    if xaxis_length <= 95:
        return 242.192 - 1.91781 * xaxis_length
    elif xaxis_length > 95:
        return 67.4074 - 0.0779727 * xaxis_length


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
    Write the Pick a Contact page.
    """
    logger.info(f'Writing page {code("Pick a Contact")}', bold=True)
    st.image(join(root_dir, 'graphics', 'pick_a_contact.png'))

    #
    # Page preparation
    #

    show_group_chats = st.checkbox('Show group chats', value=False, help="Include group chat names in the 'Contact name' dropdown?")

    col1, col2 = st.columns(2)

    contact_names_display = data.lst_contact_names_all if show_group_chats else data.lst_contact_names_no_group_chats
    contact_names_display = [x for x in contact_names_display if len(x.strip()) > 0]
    assert len(contact_names_display), 'No contact names in contact.csv or chat identifiers. Something is catastrophically wrong.'

    contact_name = col1.selectbox(
        label='Contact name',
        options=contact_names_display,
        # index=0,
        index=contact_names_display.index('Maria Sooklaris'),  # TODO reset to above line
        help="Choose a contact you'd like to analyze data for!"
    )

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
    first_message_dt = pdata['daily_summary']['dt'].min()
    last_message_dt = pdata['daily_summary']['dt'].max()

    # Add filters now that first and last message dates are known
    use_exact_dates = st.checkbox(
        label='Use exact dates',
        value=False,
        help="Show date filter as a relative date dropdown select list, instead of specific start/stop date inputs?"
    )

    # Create adaptive date input
    filter_start_dt, filter_stop_dt = adaptive_date_input(use_exact_dates, first_message_dt, last_message_dt)

    # Filter page data
    pdata = filter_page_data(pdata, filter_start_dt, filter_stop_dt, contact_name)

    # Add control for types of messages to display
    selected_include_type_columns = message_type_input(pdata)

    # Add column that reflects sum of selected message types
    count_col = 'display_message_count'
    pdata['summary'][count_col] = pdata['summary'][selected_include_type_columns].sum(axis=1)
    pdata['summary_from_who'][count_col] = pdata['summary_from_who'][selected_include_type_columns].sum(axis=1)
    pdata['daily_summary'][count_col] = pdata['daily_summary'][selected_include_type_columns].sum(axis=1)
    pdata['daily_summary_all_contacts'][count_col] = pdata['daily_summary_all_contacts'][selected_include_type_columns].sum(axis=1)
    pdata['daily_summary_from_who'][count_col] = pdata['daily_summary_from_who'][selected_include_type_columns].sum(axis=1)

    # Add a dataframe called 'summary_resample' to page data dictionary based on selected date granularity
    pdata = resample_page_data(pdata, dt_gran)

    # Clean up variables not used in the rest of the function
    del dt_options, use_exact_dates, selected_include_type_columns


    #
    # Page sections
    #

    class Visuals():
        """
        Visuals for the Pick a Contact page.
        """
        def __init__(self, contact_name, data, pdata, dt_gran, count_col):
            self.contact_name = contact_name
            self.data = data
            self.pdata = pdata
            self.dt_gran = dt_gran
            self.count_col = count_col
            self.tooltip_dt_title, self.tooltip_dt_format, self.xaxis_identifier, self.dt_offset = get_altair_dt_plot_attributes(dt_gran)

        def filter_contact(self, df: pd.DataFrame):
            """
            Filter a given dataframe for the contact name in this class definition.
            """
            return df.loc[df['contact_name'] == self.contact_name]

        def filter_from_me(self, df: pd.DataFrame):
            """
            Filter a given dataframe for messages from me.
            """
            return df.loc[df['is_from_me'] == 1]

        def filter_from_them(self, df: pd.DataFrame):
            """
            Filter a given dataframe for messages from them.
            """
            return df.loc[df['is_from_me'] == 0]

        def section_statistics_first_and_last_message_sent_dt(self):
            st.markdown(f"""
            First message with **{self.contact_name}** exchanged on
            **{to_date_str(self.pdata['summary']['first_message_dt'].squeeze())}**,
            latest message exchanged on
            **{to_date_str(self.pdata['summary']['latest_message_dt'].squeeze())}**.
            Last contact was
            **{humanize.naturaltime(pd.to_datetime(self.pdata['summary']['latest_message_ts'].squeeze()).replace(tzinfo=None))}**.
            """)

        def section_message_volume(self):

            def text_total_message_volume():
                total = self.pdata['summary'][self.count_col].squeeze()
                total_from_me = self.filter_from_me(self.pdata['summary_from_who'])[self.count_col].squeeze()
                total_from_them = self.filter_from_them(self.pdata['summary_from_who'])[self.count_col].squeeze()

                st.markdown(f"""Total messages exchanged with **{contact_name}**,
                **{str(int(round(total_from_me/total * 100, 0))) + '%'}** sent by me,
                **{str(int(round(total_from_them/total * 100, 0))) + '%'}** sent by them.
                """)
                st.markdown(csstext(intword(total), cls='large-text-green-center'), unsafe_allow_html=True)

            def bar_message_volume():
                st.markdown(f"Here's our total message volume over time, shown by **{self.dt_gran}**:")

                df_message_volume = self.pdata['summary_resample'].copy()
                df_message_volume['dt'] = df_message_volume['dt'] + self.dt_offset
                brush = alt.selection_interval(encodings=['x'])

                st.altair_chart(
                    alt.Chart(data=df_message_volume.sort_values('dt'), background=color.background_main)
                    .mark_bar(
                        cornerRadiusTopLeft=get_corner_radius_size(len(df_message_volume)),
                        cornerRadiusTopRight=get_corner_radius_size(len(df_message_volume))
                    ).encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor=color.xaxis_label)),
                        y=alt.Y(self.count_col, title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
                        color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
                        tooltip=[
                            alt.Tooltip(self.count_col, title='Messages'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ]
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .add_selection(brush)
                    .properties(width=600, height=300)
                )

            def line_gradient_message_volume_day_of_week():
                message_volume_dow = self.pdata['daily_summary'][['dt', self.count_col]].reset_index().copy()
                message_volume_dow['weekday'] = message_volume_dow['dt'].dt.day_name()
                message_volume_dow = message_volume_dow.drop('dt', axis=1).groupby('weekday').mean()
                most_popular_day = message_volume_dow[self.count_col].idxmax()
                n_messages_on_most_popular_day = message_volume_dow[self.count_col].max()

                st.markdown("Here's the average number of messages we've exchanged per day of the week:")

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
                        y=alt.Y(self.count_col, title=None, axis=alt.Axis(labelColor=color.xaxis_label)),
                        tooltip=[
                            alt.Tooltip(self.count_col, title='Average messages', format='.1f'),
                            alt.Tooltip('weekday', title='Weekday'),
                        ]
                    )
                    .configure_axis(grid=False)
                    .configure_axisX(labelAngle=0)
                    .configure_view(strokeOpacity=0)
                    .properties(width=600, height=150)
                )

                st.markdown(f'Looks like **{most_popular_day}** is our most active texting day, with an average of **{round(n_messages_on_most_popular_day, 1)}** messages exchanged that day.')

            def line_percent_of_my_messages():
                st.markdown(csstext('% of All My Messages', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
                st.markdown(f'Percent of my total volume (across all contacts) made up to & from **{self.contact_name}**:')

                pct_message_volume = (
                    self.pdata['summary_resample']
                    [['dt', self.count_col]]
                    .rename(columns={self.count_col: 'messages_contact'})
                    .merge(
                        self.pdata['summary_all_contacts_resample']
                        .groupby('dt')
                        .sum()
                        .reset_index()
                        [['dt', self.count_col]]
                        .rename(columns={self.count_col: 'messages_all_contacts'}),
                        on='dt'
                    )
                )

                pct_message_volume['rate'] = pct_message_volume['messages_contact'] / pct_message_volume['messages_all_contacts']

                chart_df = pct_message_volume.copy()
                chart_df.index = chart_df.reset_index()['dt'] + self.dt_offset
                brush = alt.selection_interval(encodings=['x'])

                st.altair_chart(
                    alt.Chart(data=chart_df['rate'].sort_index().reset_index(), background='#2b2b2b')
                    .mark_line(size=3, point=dict(filled=False, fill='darkslategray'))
                    .encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor='dimgray')),
                        y=alt.Y('rate', title=None, axis=alt.Axis(format='%', labelColor='dimgray')),
                        color=alt.condition(brush, alt.value('#83cf83'), alt.value('lightgray')),
                        tooltip=[
                            alt.Tooltip('rate', title='Percent', format='.2%'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ]
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .configure_point(size=get_point_size(len(chart_df)))
                    .add_selection(brush)
                    .properties(width=600, height=300)
                )


            st.markdown(csstext('Message Volume', cls='medium-text-bold', header=True), unsafe_allow_html=True)

            text_total_message_volume()
            bar_message_volume()
            line_gradient_message_volume_day_of_week()
            line_percent_of_my_messages()

        def section_words(self):

            def text_total_word_volume():
                total = self.pdata['summary']['tokens'].squeeze()
                total_from_me = self.filter_from_me(self.pdata['summary_from_who'])['tokens'].squeeze()
                total_from_them = self.filter_from_them(self.pdata['summary_from_who'])['tokens'].squeeze()

                st.markdown(f"""Total words exchanged,
                **{str(int(round(total_from_me/total * 100, 0))) + '%'}** written by me,
                **{str(int(round(total_from_them/total * 100, 0))) + '%'}** written by them.
                """)
                st.markdown(csstext(intword(total), cls='large-text-green-center'), unsafe_allow_html=True)

            def wordcloud_word_volume():
                # Pull data
                df_word_counts = (
                    self.data.contact_token_usage_from_who_vw
                    .loc[self.data.contact_token_usage_from_who_vw['contact_name'] == self.contact_name]
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

                tmp_wordcloud_from_me_fpath = 'tmp_imessage_extractor_app_pick_a_contact_wordcloud_from_me.csv'
                tmp_wordcloud_from_them_fpath = 'tmp_imessage_extractor_app_pick_a_contact_wordcloud_from_them.csv'

                (
                    df_word_counts
                     .loc[self.data.contact_token_usage_from_who_vw['is_from_me'] == 1]
                    [['token', 'usages']]
                    .to_csv(tmp_wordcloud_from_me_fpath, index=False)
                )

                (
                    df_word_counts
                     .loc[self.data.contact_token_usage_from_who_vw['is_from_me'] == 0]
                    [['token', 'usages']]
                    .to_csv(tmp_wordcloud_from_them_fpath, index=False)
                )

                icon_name = 'fas fa-comment'
                expected_stylecloud_from_me_fpath = 'stylecloud_pick_a_contact_from_me.png'
                expected_stylecloud_from_them_fpath = 'stylecloud_pick_a_contact_from_them.png'

                stylecloud.gen_stylecloud(file_path=tmp_wordcloud_from_me_fpath,
                                          icon_name=icon_name,
                                          colors=[color.imessage_green, '#a6e0a6', '#dcecdc', '#ffffff'],
                                          background_color=color.background_main,
                                          output_name=expected_stylecloud_from_me_fpath,
                                          gradient='horizontal',
                                          max_words=500,
                                          stopwords=True,
                                          custom_stopwords=self.data.lst_contractions_wo_apostrophe + stopwords.words('english'),
                                          size=(1024, 800))

                stylecloud.gen_stylecloud(file_path=tmp_wordcloud_from_them_fpath,
                                          icon_name=icon_name,
                                          colors=[color.imessage_green, '#a6e0a6', '#dcecdc', '#ffffff'],
                                          background_color=color.background_main,
                                          output_name=expected_stylecloud_from_them_fpath,
                                          gradient='horizontal',
                                          max_words=500,
                                          stopwords=True,
                                          custom_stopwords=self.data.lst_contractions_wo_apostrophe + stopwords.words('english'),
                                          size=(1024, 800))

                st.markdown(f"Here's a set of wordclouds of our most common words (excluding stopwords):")

                col1, col2 = st.columns(2)

                if isfile(expected_stylecloud_from_them_fpath):
                    col1.markdown(csstext(contact_name, cls='small22-text-center'), unsafe_allow_html=True)
                    col1.image(expected_stylecloud_from_them_fpath)
                    remove(expected_stylecloud_from_them_fpath)
                else:
                    col1.markdown(csstext('Wordcloud not available 😔', cls='medium-text-center'), unsafe_allow_html=True)

                if isfile(tmp_wordcloud_from_them_fpath):
                    remove(tmp_wordcloud_from_them_fpath)

                if isfile(expected_stylecloud_from_me_fpath):
                    col2.markdown(csstext('Me', cls='small22-text-center'), unsafe_allow_html=True)
                    col2.image(expected_stylecloud_from_me_fpath)
                    remove(expected_stylecloud_from_me_fpath)
                else:
                    col2.markdown(csstext('Wordcloud not available 😔', cls='medium-text-center'), unsafe_allow_html=True)

                if isfile(tmp_wordcloud_from_me_fpath):
                    remove(tmp_wordcloud_from_me_fpath)

            def favorite_words_by_length():
                st.markdown(csstext('Favorite words by length', cls='small22-text-center'), unsafe_allow_html=True)
                col1, col2 = st.columns(2)

                token_usage_from_who_ranked_by_length = (
                    self.data.contact_token_usage_from_who_vw
                    .loc[self.data.contact_token_usage_from_who_vw['contact_name'] == contact_name]
                    .reset_index()
                )

                token_usage_from_who_ranked_by_length['token'] = token_usage_from_who_ranked_by_length['token'].str.lower()

                # Filter out stopwords, punctuation characters and contractions
                token_usage_from_who_ranked_by_length = token_usage_from_who_ranked_by_length.loc[
                    (~token_usage_from_who_ranked_by_length['token'].isin(stopwords.words('english')))
                    # & (~token_usage_from_who_ranked_by_length['token'].isin(self.data.lst_punctuation_chars))
                    & (~token_usage_from_who_ranked_by_length['token'].isin(self.data.lst_contractions_w_apostrophe))
                    & (~token_usage_from_who_ranked_by_length['token'].str.isdigit().astype(bool))
                ]

                token_usage_from_who_ranked_by_length['rank'] = (
                    token_usage_from_who_ranked_by_length
                    .groupby(['is_from_me', 'length'])
                    ['usages']
                    .rank(method='first', ascending=False)
                )

                self.pdata['top_token_usage_from_who_by_length'] = (
                    token_usage_from_who_ranked_by_length
                    .loc[
                        (token_usage_from_who_ranked_by_length['rank'] == 1)
                        & (token_usage_from_who_ranked_by_length['length'] <= 10)
                    ]
                    .drop('rank', axis=1)
                    .sort_values(['is_from_me', 'length'])
                )

                brush = alt.selection_interval(encodings=['y'])

                df_chart_from_me = (
                    self.pdata['top_token_usage_from_who_by_length']
                    .loc[self.pdata['top_token_usage_from_who_by_length']['is_from_me'] == True]
                    .sort_values('length')
                )

                df_chart_from_them = (
                    self.pdata['top_token_usage_from_who_by_length']
                    .loc[self.pdata['top_token_usage_from_who_by_length']['is_from_me'] == False]
                    .sort_values('length')
                )

                col1, col2 = st.columns(2)

                col1.altair_chart(
                    alt.Chart(data=df_chart_from_them, background='#2b2b2b')
                    .mark_bar(size=3, point=dict(filled=False, fill='darkslategray'))
                    .mark_bar(
                        cornerRadiusTopRight=3,
                        cornerRadiusBottomRight=3
                    )
                    .encode(
                        x=alt.X('usages', title=None, axis=alt.Axis(labelColor='dimgray')),
                        y=alt.Y('token', title=None, sort=list(df_chart_from_them['token']), axis=alt.Axis(labelColor='dimgray')),
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
                    .properties(width=300, height=300),
                    use_container_width=False
                )

                col2.altair_chart(
                    alt.Chart(data=df_chart_from_me, background='#2b2b2b')
                    .mark_bar(size=3, point=dict(filled=False, fill='darkslategray'))
                    .mark_bar(
                        cornerRadiusTopRight=3,
                        cornerRadiusBottomRight=3
                    )
                    .encode(
                        x=alt.X('usages', title=None, axis=alt.Axis(labelColor='dimgray')),
                        y=alt.Y('token', title=None, sort=list(df_chart_from_me['token']), axis=alt.Axis(labelColor='dimgray')),
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
                    .properties(width=300, height=300),
                    use_container_width=False
                )

            def average_words_per_message():
                # TODO: Make this and the following function into a grouped bar chart, with three bars:
                # avg words/message, avg chars/message avg chars/word

                avg_words_per_message_from_me = \
                    (self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 1]['tokens'].sum()
                    / self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 1]['text_messages'].sum())

                avg_words_per_message_from_them = \
                    (self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 0]['tokens'].sum()
                    / self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 0]['text_messages'].sum())

                st.markdown(csstext('Average words per message', cls='small22-text-center'), unsafe_allow_html=True)

                col1, col2 = st.columns(2)
                col1.markdown(csstext(round(avg_words_per_message_from_them, 1), cls='large-text-green-center'), unsafe_allow_html=True)
                col2.markdown(csstext(round(avg_words_per_message_from_me, 1), cls='large-text-green-center'), unsafe_allow_html=True)

            def average_characters_per_message():
                avg_chars_per_message_from_me = \
                    (self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 1]['characters'].sum()
                    / self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 1]['text_messages'].sum())

                avg_chars_per_message_from_them = \
                    (self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 0]['characters'].sum()
                    / self.pdata['daily_summary_from_who'].loc[self.pdata['daily_summary_from_who']['is_from_me'] == 0]['text_messages'].sum())

                st.markdown(csstext('Average characters per message', cls='small22-text-center'), unsafe_allow_html=True)

                col1, col2 = st.columns(2)
                col1.markdown(csstext(int(round(avg_chars_per_message_from_them, 0)), cls='large-text-green-center'), unsafe_allow_html=True)
                col2.markdown(csstext(int(round(avg_chars_per_message_from_me, 0)), cls='large-text-green-center'), unsafe_allow_html=True)


            st.markdown(csstext('Words', cls='medium-text-bold', header=True), unsafe_allow_html=True)

            text_total_word_volume()
            wordcloud_word_volume()
            favorite_words_by_length()
            average_words_per_message()
            average_characters_per_message()

        def section_special_messages(self):

            def bar_threads():
                st.markdown(csstext('Threads', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
                st.markdown(f"""Number of messages each of us sent that either
                <b><font color="{color.imessage_purple}">resulted in a new thread being created</font></b>,
                (by the other responding to that message), or
                <b><font color="{color.imessage_green}">replied to an existing thread</font></b>.
                Threads were introduced to iMessage in November '20.
                """, unsafe_allow_html=True)

                df = (
                    self.pdata['summary_from_who_resample']
                    [['is_from_me', 'dt', 'thread_origins', 'threaded_replies']]
                    .copy()
                )

                df['dt'] = df['dt'] + self.dt_offset

                df_from_me = df.loc[df['is_from_me'] == 1].drop('is_from_me', axis=1).copy()
                df_from_me = df_from_me.melt(id_vars='dt', value_vars=['thread_origins', 'threaded_replies'], var_name='Thread Type', value_name='count')
                df_from_me['Thread Type'] = df_from_me['Thread Type'].map({'thread_origins': 'Origins', 'threaded_replies': 'Replies'})

                df_from_them = df.loc[df['is_from_me'] == 0].drop('is_from_me', axis=1).copy()
                df_from_them = df_from_them.melt(id_vars='dt', value_vars=['thread_origins', 'threaded_replies'], var_name='Thread Type', value_name='count')
                df_from_them['Thread Type'] = df_from_them['Thread Type'].map({'thread_origins': 'Origins', 'threaded_replies': 'Replies'})

                max_y_value = max(df_from_me.groupby('dt').sum()['count'].max(), df_from_them.groupby('dt').sum()['count'].max())

                col1, col2 = st.columns(2)

                col1.markdown(csstext(contact_name, cls='small22-text-center'), unsafe_allow_html=True)
                col2.markdown(csstext('Me', cls='small22-text-center'), unsafe_allow_html=True)

                col1.altair_chart(
                    alt.Chart(data=df_from_them.sort_values('dt'), background=color.background_main)
                    .mark_bar(
                        cornerRadiusTopLeft=get_corner_radius_size(len(df_from_them)),
                        cornerRadiusTopRight=get_corner_radius_size(len(df_from_them)),
                    ).encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor=color.xaxis_label)),
                        y=alt.Y(
                            'count',
                            title=None,
                            scale=alt.Scale(domain=[0, max_y_value]),
                            axis=alt.Axis(labelColor=color.xaxis_label)
                        ),
                        color=alt.Color(
                            'Thread Type',
                            scale=alt.Scale(domain=['Origins', 'Replies'], range=[color.imessage_purple, color.imessage_green]),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip('count', title='Messages'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ],
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .properties(width=300, height=220)
                )

                col2.altair_chart(
                    alt.Chart(data=df_from_me.sort_values('dt'), background=color.background_main)
                    .mark_bar(
                        cornerRadiusTopLeft=get_corner_radius_size(len(df_from_me)),
                        cornerRadiusTopRight=get_corner_radius_size(len(df_from_me)),
                    ).encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor=color.xaxis_label)),
                        y=alt.Y(
                            'count',
                            title=None,
                            scale=alt.Scale(domain=[0, max_y_value]),
                            axis=alt.Axis(labelColor=color.xaxis_label)
                        ),
                        color=alt.Color(
                            'Thread Type',
                            scale=alt.Scale(domain=['Origins', 'Replies'], range=[color.imessage_purple, color.imessage_green]),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip('count', title='Messages'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ],
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .properties(width=300, height=220)
                )

            def bar_apps_for_imessage():
                st.markdown(csstext('Apps for iMessage', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
                st.markdown("""
                Our usage of Apps for iMessage, includes <b>Fitness</b> notifications, <b>Game Pigeon</b> plays, <b>Apple Cash</b> sends and
                requests, <b>polls</b>, <b>stickers</b> and any other app that's integrated into the native Messages app.
                """, unsafe_allow_html=True)

                df = self.pdata['summary_from_who_resample'][['is_from_me', 'dt', 'app_for_imessage']]
                df['dt'] = df['dt'] + self.dt_offset

                df_from_me = df.loc[df['is_from_me'] == 1].copy()
                df_from_them = df.loc[df['is_from_me'] == 0].copy()

                max_y_value = max(df_from_me['app_for_imessage'].max(), df_from_them['app_for_imessage'].max())

                col1, col2 = st.columns(2)

                col1.markdown(csstext(contact_name, cls='small22-text-center'), unsafe_allow_html=True)
                col2.markdown(csstext('Me', cls='small22-text-center'), unsafe_allow_html=True)

                brush = alt.selection_interval(encodings=['x'])

                col1.altair_chart(
                    alt.Chart(data=df_from_them.sort_values('dt'), background=color.background_main)
                    .mark_bar(
                        cornerRadiusTopLeft=get_corner_radius_size(len(df_from_them)),
                        cornerRadiusTopRight=get_corner_radius_size(len(df_from_them))
                    ).encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor=color.xaxis_label)),
                        y=alt.Y(
                            'app_for_imessage',
                            title=None,
                            scale=alt.Scale(domain=[0, max_y_value]),
                            axis=alt.Axis(labelColor=color.xaxis_label)
                        ),
                        color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
                        tooltip=[
                            alt.Tooltip('app_for_imessage', title='App Messages'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ],
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .add_selection(brush)
                    .properties(width=300, height=220)
                )

                col2.altair_chart(
                    alt.Chart(data=df_from_me.sort_values('dt'), background=color.background_main)
                    .mark_bar(
                        cornerRadiusTopLeft=get_corner_radius_size(len(df_from_me)),
                        cornerRadiusTopRight=get_corner_radius_size(len(df_from_me))
                    ).encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor=color.xaxis_label)),
                        y=alt.Y(
                            'app_for_imessage',
                            title=None,
                            scale=alt.Scale(domain=[0, max_y_value]),
                            axis=alt.Axis(labelColor=color.xaxis_label)
                        ),
                        color=alt.condition(brush, alt.value(color.imessage_green), alt.value('gray')),
                        tooltip=[
                            alt.Tooltip('app_for_imessage', title='App Messages'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ],
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .add_selection(brush)
                    .properties(width=300, height=220)
                )

            def bar_image_attachments():
                st.markdown(csstext('Image Attachments', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
                st.markdown(f"""Number of
                <b><font color="{color.imessage_purple}">messages with images attached</font></b>
                that we've sent, and
                <b><font color="{color.imessage_green}">images sent by themselves</font></b>
                .
                """, unsafe_allow_html=True)

                df = (
                    self.pdata['summary_from_who_resample']
                    [['is_from_me', 'dt', 'messages_containing_attachment_image', 'messages_image_attachment_only']]
                )
                df['dt'] = df['dt'] + self.dt_offset

                df_from_me = df.loc[df['is_from_me'] == 1].copy()
                df_from_me = df_from_me.melt(id_vars='dt', value_vars=['messages_containing_attachment_image', 'messages_image_attachment_only'], var_name='Attachment Type', value_name='count')
                df_from_me['Attachment Type'] = df_from_me['Attachment Type'].map({'messages_containing_attachment_image': 'Messages with Attachments', 'messages_image_attachment_only': 'Attachments Only'})

                df_from_them = df.loc[df['is_from_me'] == 0].copy()
                df_from_them = df_from_them.melt(id_vars='dt', value_vars=['messages_containing_attachment_image', 'messages_image_attachment_only'], var_name='Attachment Type', value_name='count')
                df_from_them['Attachment Type'] = df_from_them['Attachment Type'].map({'messages_containing_attachment_image': 'Messages with Attachments', 'messages_image_attachment_only': 'Attachments Only'})

                max_y_value = max(df_from_me.groupby('dt').sum()['count'].max(), df_from_them.groupby('dt').sum()['count'].max())

                col1, col2 = st.columns(2)

                col1.markdown(csstext(contact_name, cls='small22-text-center'), unsafe_allow_html=True)
                col2.markdown(csstext('Me', cls='small22-text-center'), unsafe_allow_html=True)

                col1.altair_chart(
                    alt.Chart(data=df_from_them.sort_values('dt'), background=color.background_main)
                    .mark_bar(
                        cornerRadiusTopLeft=get_corner_radius_size(len(df_from_them)),
                        cornerRadiusTopRight=get_corner_radius_size(len(df_from_them))
                    ).encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor=color.xaxis_label)),
                        y=alt.Y(
                            'count',
                            title=None,
                            scale=alt.Scale(domain=[0, max_y_value]),
                            axis=alt.Axis(labelColor=color.xaxis_label)
                        ),
                        color=alt.Color(
                            'Attachment Type',
                            scale=alt.Scale(
                                domain=['Messages with Attachments', 'Attachments Only'],
                                range=[color.imessage_purple, color.imessage_green]
                            ),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip('count', title='Attachments'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ],
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .properties(width=300, height=220)
                )

                col2.altair_chart(
                    alt.Chart(data=df_from_me.sort_values('dt'), background=color.background_main)
                    .mark_bar(
                        cornerRadiusTopLeft=get_corner_radius_size(len(df_from_me)),
                        cornerRadiusTopRight=get_corner_radius_size(len(df_from_me))
                    ).encode(
                        x=alt.X(self.xaxis_identifier, title=None, axis=alt.Axis(format=self.tooltip_dt_format, labelColor=color.xaxis_label)),
                        y=alt.Y(
                            'count',
                            title=None,
                            scale=alt.Scale(domain=[0, max_y_value]),
                            axis=alt.Axis(labelColor=color.xaxis_label)
                        ),
                        color=alt.Color(
                            'Attachment Type',
                            scale=alt.Scale(
                                domain=['Messages with Attachments', 'Attachments Only'],
                                range=[color.imessage_purple, color.imessage_green]
                            ),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip('count', title='Attachments'),
                            alt.Tooltip('dt', title=self.tooltip_dt_title, format=self.tooltip_dt_format),
                        ],
                    )
                    .configure_axis(grid=False)
                    .configure_view(strokeOpacity=0)
                    .properties(width=300, height=220)
                )

            def pie_tapbacks():
                st.markdown(csstext('Tapbacks', cls='smallmedium-text-bold', header=True), unsafe_allow_html=True)
                st.markdown("How frequently we use each flavor of tapback.")

                df = (
                    self.data.message_user
                    .reset_index()
                    .loc[self.data.message_user.reset_index()['is_emote'] == 1]
                    .groupby(['is_from_me', 'message_special_type'])
                    .agg({'message_id': 'count'})
                    .reset_index()
                    .rename(columns={'message_id': 'count', 'message_special_type': 'tapback'})
                )
                df['tapback'] = df['tapback'].replace(self.data.map_tapback_type)

                all_tapbacks = list(self.data.map_tapback_type.values())

                df_from_me = (
                    df
                    .loc[df['is_from_me'] == 1]
                    .drop('is_from_me', axis=1)
                    .merge(pd.DataFrame(all_tapbacks, columns=['tapback']), how='outer', on='tapback')
                    .fillna(0.0)
                )

                df_from_them = (
                    df
                    .loc[df['is_from_me'] == 0]
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

                fig_from_me = px.pie(
                    df_from_me,
                    hole=.5,
                    values='count',
                    names='tapback',
                    color_discrete_sequence=list(color_map.values()),
                    width=360,
                    height=340,
                )

                fig_from_me.update_traces(
                    textposition='inside',
                    textinfo='percent+label',
                    hovertemplate='Tapback: <b>%{label}</b><br>Usages: <b>%{value}</b>',
                )

                fig_from_me.update_layout(
                    showlegend=False,
                )

                fig_from_them = px.pie(
                    df_from_them,
                    hole=.5,
                    values='count',
                    names='tapback',
                    color_discrete_sequence=list(color_map.values()),
                    width=360,
                    height=340,
                )

                fig_from_them.update_traces(
                    textposition='inside',
                    textinfo='percent+label',
                    hovertemplate='Tapback: <b>%{label}</b><br>Usages: <b>%{value}</b>',
                )

                fig_from_them.update_layout(
                    showlegend=False,
                )

                col1, col2 = st.columns(2)
                col1.markdown(csstext(contact_name, cls='small22-text-center'), unsafe_allow_html=True)
                col2.markdown(csstext('Me', cls='small22-text-center'), unsafe_allow_html=True)
                col1.plotly_chart(fig_from_them)
                col2.plotly_chart(fig_from_me)


            st.markdown(csstext('Special Messages', cls='medium-text-bold', header=True), unsafe_allow_html=True)
            st.markdown('Usage breakdown of special messages types (threads, image attachments, URLs, Apps for iMessage and tapbacks).')

            bar_threads()
            bar_apps_for_imessage()
            bar_image_attachments()
            pie_tapbacks()

        # TODO next step: migrate special message section from my_stats



    visuals = Visuals(contact_name=contact_name, data=data, pdata=pdata, count_col=count_col, dt_gran=dt_gran)

    visuals.section_statistics_first_and_last_message_sent_dt()
    visuals.section_message_volume()
    visuals.section_words()
    visuals.section_special_messages()





# to add:
# - average thread length


# def write(data: 'iMessageDataExtract', logger: logging.Logger) -> None:
#     """
#     Write the Pick a Contact page.
#     """
#     logger.info(f'Writing page {code("Pick a Contact")}', bold=True)
#     st.image(join(root_dir, 'graphics', 'pick_a_contact.png'))

#     #
#     # Prepare page
#     #

#     contact_name, dt_gran, show_group_chats = controls(data)

#     inputs, page_data, stats, message_count_col, selected_include_type_columns = prepare_page_data(data, contact_name, dt_gran)

#     # Get altair plot attributes dependent on date granularity
#     tooltip_dt_title, tooltip_dt_format, xaxis_identifier, dt_offset = get_altair_dt_plot_attributes(dt_gran)

    # st.markdown(f'First message on **{to_date_str(stats["first_message_dt"])}**, latest message on **{to_date_str(stats["last_message_dt"])}**.')

#     #
#     # Message volume
#     #

#     st.markdown(csstext('Message Volume', cls='medium-text-bold', header=True), unsafe_allow_html=True)

#     viz_message_volume(data=data,
#                        page_data=page_data,
#                        contact_name=contact_name,
#                        message_count_col=message_count_col,
#                        dt_offset=dt_offset,
#                        dt_gran=dt_gran,
#                        show_group_chats=show_group_chats,
#                        xaxis_identifier=xaxis_identifier,
#                        tooltip_dt_title=tooltip_dt_title,
#                        tooltip_dt_format=tooltip_dt_format,
#                        selected_include_type_columns=selected_include_type_columns)

#     #
#     # Texting Activity
#     #

#     st.markdown(csstext('Texting Activity', cls='medium-text-bold', header=True), unsafe_allow_html=True)
#     st.markdown('At least one message exchanged on...')

#     viz_texting_activity(page_data=page_data, stats=stats, message_count_col=message_count_col)

#     st.markdown('<br>', unsafe_allow_html=True)

#     #
#     # Word Analysis
#     #

#     viz_word_analysis(data=data, page_data=page_data, stats=stats, contact_name=contact_name)

#     st.markdown('<br>', unsafe_allow_html=True)

#     #
#     # Chat History
#     #

#     st.markdown(csstext('Chat History', cls='medium-text-bold', header=True), unsafe_allow_html=True)
#     st.markdown(f'Snapshot of my message history with **{contact_name}**, most recent messages first.')

#     viz_tabular_chat_history(data=data, inputs=inputs, contact_name=contact_name)

#     #
#     # Wrap up
#     #

    logger.info('Done', arrow='black')
