drop view if exists summary_contact_from_who_vw;
create view summary_contact_from_who_vw as

with ts_statistics as (
    select contact_name
           , is_from_me
           , min(ts) as first_message_ts
           , max(ts) as latest_message_ts
    from message_user
    group by contact_name, is_from_me
),

main_statistics as (
    select contact_name
           , is_from_me
           , sum(messages) as messages
           , sum(text_messages) as text_messages
           , sum(group_chat_messages) as group_chat_messages
           , sum(group_chat_text_messages) as group_chat_text_messages
           , sum(imessages) as imessages
           , sum(sms) as sms
           , sum(emotes) as emotes
           , sum(emotes_love) as emotes_love
           , sum(emotes_likes) as emotes_likes
           , sum(emotes_dislikes) as emotes_dislikes
           , sum(emotes_laugh) as emotes_laugh
           , sum(emotes_emphasis) as emotes_emphasis
           , sum(emotes_question) as emotes_question
           , sum(emotes_remove_love) as emotes_remove_love
           , sum(emotes_remove_like) as emotes_remove_like
           , sum(emotes_remove_dislike) as emotes_remove_dislike
           , sum(emotes_remove_laugh) as emotes_remove_laugh
           , sum(emotes_remove_emphasis) as emotes_remove_emphasis
           , sum(emotes_remove_question) as emotes_remove_question
           , sum(urls) as urls
           , sum(app_for_imessage) as app_for_imessage
           , sum(thread_origins) as thread_origins
           , sum(threaded_replies) as threaded_replies
           , sum(messages_containing_attachment) as messages_containing_attachment
           , sum(messages_attachment_only) as messages_attachment_only
           , sum(messages_containing_attachment_image) as messages_containing_attachment_image
           , sum(messages_image_attachment_only) as messages_image_attachment_only
           , sum(tokens) as tokens
           , sum(characters) as characters
           , count(distinct dt) as dates_messaged
           , min(dt) as first_message_dt
           , max(dt) as latest_message_dt
    from daily_summary_contact_from_who_vw
    group by contact_name, is_from_me
)

select m.*, t.first_message_ts, t.latest_message_ts
from main_statistics m
left join ts_statistics t
  on m.contact_name = t.contact_name
  and m.is_from_me = t.is_from_me
