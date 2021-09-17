"""Pick a Contact page"""
import streamlit as st
import datetime
import pandas as pd
from pydoni import advanced_strip


def to_date_str(dt: datetime.datetime) -> str:
    """
    Convert a date to a string in desired human-readable format.
    """
    if isinstance(dt, datetime.datetime) or isinstance(dt, datetime.date):
        return dt.strftime('%b %-d, %Y')
    elif isinstance(dt, str):
        try:
            return datetime.datetime.strptime(dt, '%Y-%m-%d').strftime('%b %-d, %Y')
        except ValueError:
            print(f'"{str(dt)}" (type: "{type(dt)}") is not a datetime or datetime-coercible object')
            return 'N/A'
    else:
        print(f'"{str(dt)}" (type: "{type(dt)}") is not a datetime or datetime-coercible object')
        return 'N/A'

def wrap_tag(tag: str, text: str) -> str:
    return f'<{tag}>{str(text)}</{tag}>'


def span(text: str) -> str:
    return wrap_tag('span', str(text))


def largetext(text: str) -> str:
    return wrap_tag('largetext', str(text))


def largetextcolor(text: str) -> str:
    return wrap_tag('largetextcolor', str(text))


def mediumtext(text: str) -> str:
    return wrap_tag('mediumtext', str(text))


def mediumtextcolor(text: str) -> str:
    return wrap_tag('mediumtextcolor', str(text))


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
    dt_gran = col2.selectbox('Date granularity', ['Day', 'Week', 'Month', 'Year'], help='Determine the date granularity of the visualizations below')
    include_types = col2.multiselect('Include message types', ['iMessage', 'SMS', 'Emote', 'App for iMessage'],
                                     ['iMessage', 'SMS', 'Emote', 'App for iMessage'],
                                     help=advanced_strip("""Select the message types to be included
                                     in the analysis below. By default, all message types are included.
                                     'Emote' includes all tapback replies (i.e. likes, dislikes, hearts, etc.).
                                     'App for iMessage' includes other messages sent via apps
                                     integrated into iMessage (i.e. Workout notifications, Apple Cash payments, etc.).
                                     """))

    # TODO: actually apply include_types

    # Subset the daily summary for the selected contact
    daily_summary = data.daily_summary_contact[data.daily_summary_contact.index.get_level_values('contact_name') == contact_name].droplevel('contact_name')
    daily_summary.index = pd.to_datetime(daily_summary.index)

    # Get max and min message dates for this contact
    first_message_dt = daily_summary.index.get_level_values('dt').min()
    last_message_dt = daily_summary.index.get_level_values('dt').max()

    # Add filter now that first and last message dates are known
    filter_start_dt, filter_stop_dt = col1.date_input('Date range', [first_message_dt, last_message_dt],
        help='Filter the date range to anaylyze data for? Defaults to the first/last dates for this contact.')
    daily_summary = daily_summary.loc[filter_start_dt:filter_stop_dt]

    # High-level stats before any resampling is done
    active_texting_days = daily_summary.shape[0]
    total_active_texting_days = len(data.message_vw.loc[(data.message_vw['dt'] >= filter_start_dt)
                                    & (data.message_vw['dt'] <= filter_stop_dt)]['dt'].unique())
    active_texting_days_pct = round(active_texting_days / total_active_texting_days * 100, 1)

    # Resample daily_summary to selected date granularity
    if dt_gran == 'Week':
        daily_summary = daily_summary.resample('W-SUN').sum()
    elif dt_gran == 'Month':
        daily_summary = daily_summary.resample('M').sum()
    elif dt_gran == 'Year':
        daily_summary = daily_summary.resample('Y').sum()

    st.markdown('# Active Texting Days')
    st.write('At least one message exchanged on...')

    st.markdown(span(f"""
    {largetextcolor(active_texting_days)} {mediumtext('active texting days')}
    """),
    unsafe_allow_html=True)

    st.markdown(span(f"""
    {mediumtext('Active on')}
    {mediumtextcolor(str(active_texting_days_pct) + '%')}
    {mediumtext('of days in selected time window')}
    """),
    unsafe_allow_html=True)

    st.markdown('<br>', unsafe_allow_html=True)
    st.write(f'First message on **{to_date_str(first_message_dt)}**, latest message on **{to_date_str(last_message_dt)}**')

    st.markdown('## Message Volume')

    st.bar_chart(daily_summary['n_messages'])

    st.markdown('## Percent of Message Volume')

    # Compute % of total texting volume made up by this contact
    pct_message_volume = pd.concat([
            daily_summary['n_messages'].rename('n_messages_contact'),
            data.daily_summary['n_messages'],
        ],
        axis=1
    ).fillna(0.)

    pct_message_volume['pct_volume'] = pct_message_volume['n_messages_contact'] / pct_message_volume['n_messages'] * 100

    st.line_chart(pct_message_volume['pct_volume'])


    # add "view messages for a given date or date range"

    logger.info('Finished writing page "Pick a Contact"')
