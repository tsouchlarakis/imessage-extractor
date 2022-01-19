import streamlit as st
import stylecloud
from nltk.corpus import stopwords
from imessage_extractor.src.app.helpers import intword, csstext, htmlbold
from os import remove
from os.path import isfile


def viz_wordcloud(data, page_data, stats, contact_name) -> None:
    """
    Create the visualization.
    """
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