drop view if exists daily_summary_contact_from_who_vw;
create view daily_summary_contact_from_who_vw as

with m as (
  select dt
         , contact_name
         , is_from_me
         , count(message_id) as messages
         , count(case when is_text = 1 then message_id else null end) as text_messages
         , count(case when is_group_chat = 1 then message_id else null end) as group_chat_messages
         , count(case when is_text = 1 and is_group_chat = 1 then message_id else null end) as group_chat_text_messages
         , count(case when service = 'iMessage' then message_id else null end) as imessages
         , count(case when service = 'SMS' then message_id else null end) as sms
         , count(case when is_emote = 1 then message_id else null end) as emotes
         , count(case when message_special_type = 'emote_love' then message_id else null end) as emotes_love
         , count(case when message_special_type = 'emote_like' then message_id else null end) as emotes_likes
         , count(case when message_special_type = 'emote_dislike' then message_id else null end) as emotes_dislikes
         , count(case when message_special_type = 'emote_laugh' then message_id else null end) as emotes_laugh
         , count(case when message_special_type = 'emote_emphasis' then message_id else null end) as emotes_emphasis
         , count(case when message_special_type = 'emote_question' then message_id else null end) as emotes_question
         , count(case when message_special_type = 'emote_remove_heart' then message_id else null end) as emotes_remove_love
         , count(case when message_special_type = 'emote_remove_like' then message_id else null end) as emotes_remove_like
         , count(case when message_special_type = 'emote_remove_dislike' then message_id else null end) as emotes_remove_dislike
         , count(case when message_special_type = 'emote_remove_laugh' then message_id else null end) as emotes_remove_laugh
         , count(case when message_special_type = 'emote_remove_emphasis' then message_id else null end) as emotes_remove_emphasis
         , count(case when message_special_type = 'emote_remove_question' then message_id else null end) as emotes_remove_question
         , count(case when is_url = 1 then message_id else null end) as urls
         , count(case when message_special_type is not null and is_emote = 0 then message_id else null end) as app_for_imessage
         , count(case when is_thread_origin = 1 then message_id else null end) as thread_origins
         , count(case when is_threaded_reply = 1 then message_id else null end) as threaded_replies
         , count(case when has_attachment = 1 then message_id else null end) as messages_containing_attachment
         , count(case when is_attachment = 1 then message_id else null end) as messages_attachment_only
         , count(case when has_attachment_image = 1 then message_id else null end) as messages_containing_attachment_image
         , count(case when is_attachment_image = 1 then message_id else null end) as messages_image_attachment_only
  from message_user
  group by dt, contact_name, is_from_me
),

t as (
  select dt
         , contact_name
         , is_from_me
         , sum(n_tokens) as tokens
         , sum(n_characters) as characters
  from message_user_text_vw
  group by dt, contact_name, is_from_me
)

select m.*, t.tokens, t.characters
from m
left join t
  on m.dt = t.dt
  and m.contact_name = t.contact_name
  and m.is_from_me = t.is_from_me