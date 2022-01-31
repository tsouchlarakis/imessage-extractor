import pandas as pd
import streamlit as st


def viz_tabular_chat_history(data, inputs, contact_name) -> None:
    """
    Create the visualization.
    """
    message_snapshot = (
        data.message_user
        [['ts', 'text', 'is_from_me']]
        .loc[(data.message_user['is_text'])
             & (~data.message_user['is_empty'])
             & (data.message_user['contact_name'] == contact_name)
             & (data.message_user['dt'] >= pd.to_datetime(inputs['filter_start_dt']))
                & (data.message_user['dt'] <= pd.to_datetime(inputs['filter_stop_dt']))]
    )

    col1, col2 = st.columns((1, 2.5))

    n_message_display = col1.number_input('Show this many messages', min_value=1, max_value=len(message_snapshot), value=min(20, len(message_snapshot)), step=1)

    message_snapshot_display = pd.concat([
        (
            message_snapshot
            .loc[message_snapshot['is_from_me'] == 1]
            .rename(columns={'text': 'Me'})
            [['ts', 'Me']]
        ),
        (
            message_snapshot
            .loc[message_snapshot['is_from_me'] == 0]
            .rename(columns={'text': contact_name})
            [['ts', contact_name]]
        )
    ], axis=0).sort_values('ts', ascending=False)

    message_snapshot_display['Time'] = message_snapshot_display['ts'].dt.strftime("%b %-d '%y at %I:%M:%S %p").str.lower().str.capitalize()

    st.write(message_snapshot_display[['Time', contact_name, 'Me']].set_index('Time').fillna('').head(n_message_display))
