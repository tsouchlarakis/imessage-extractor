drop view if exists {pg_schema}.daily_summary_contact_from_who;
create or replace view {pg_schema}.daily_summary_contact_from_who as

with m as (
  select dt
         , contact_name
         , is_from_me
         , count(message_id) as messages
         , count(case when is_text = true then message_id else null end) as text_messages
         , count(case when is_group_chat = true then message_id else null end) as group_chat_messages
         , count(case when is_text = true and is_group_chat = true then message_id else null end) as group_chat_text_messages
         , count(case when service = 'iMessage' then message_id else null end) as imessages
         , count(case when service = 'SMS' then message_id else null end) as sms
         , count(case when is_emote = true then message_id else null end) as emotes
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
         , count(case when is_url = true then message_id else null end) as urls
         , count(case when message_special_type is not null and is_emote = false then message_id else null end) as app_for_imessage
         , count(case when is_thread_origin = true then message_id else null end) as thread_origins
         , count(case when is_threaded_reply = true then message_id else null end) as threaded_replies
         , count(case when has_attachment = true then message_id else null end) as messages_containing_attachments
         , count(case when has_attachment = true and is_text = false and is_emote = false and is_url = false and message_special_type is null then message_id else null end) as messages_attachments_only
  from {pg_schema}.message_vw
  group by dt, contact_name, is_from_me

),

t as (
  select dt
         , contact_name
         , is_from_me
         , sum(n_tokens) as tokens
         , sum(n_characters) as characters
  from {pg_schema}.message_vw_text
  group by dt, contact_name, is_from_me
)

select m.*, t.tokens, t.characters
from m
left join t
  on m.dt = t.dt
  and m.contact_name = t.contact_name
  and m.is_from_me = t.is_from_me