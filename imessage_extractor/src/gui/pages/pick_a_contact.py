"""Pick a Contact page"""
import streamlit as st
import copy


to_date_str = lambda dt: dt.strftime('%b %-d, %Y') if dt is not None else ''


def write(data) -> None:
    """
    Write the Pick a Contact page.
    """
    st.markdown('<p class="big-font">Pick a Contact</p>', unsafe_allow_html=True)

    data = copy.deepcopy(data)

    contact_name = st.selectbox('Contact name', data.lst_all_contact_names)

    data.message_vw = data.message_vw.loc[data.message_vw['contact_name'] == contact_name]

    active_texting_days = len(data.message_vw['dt'].unique())
    first_message_dt = data.message_vw['dt'].min()
    last_message_dt = data.message_vw['dt'].max()

    st.markdown('# Active Texting Days')
    st.write('At least one message exchanged')

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

    st.markdown(span(f'{largetextcolor(active_texting_days)} {mediumtext("active texting days")}'), unsafe_allow_html=True)

    st.write(f'First message on **{to_date_str(first_message_dt)}**, latest message on **{to_date_str(last_message_dt)}**')

    st.bar_chart(data.message_vw.groupby('dt').size())

    st.line_chart(data.message_vw.groupby('dt'))


    st.bar_chart(data.contact_active_texting_days.head(10))
