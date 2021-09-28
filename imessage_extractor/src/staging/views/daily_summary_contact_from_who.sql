drop view if exists {pg_schema}.daily_summary_contact_from_who;
create or replace view {pg_schema}.daily_summary_contact_from_who as

select dt
       , contact_name
       , is_from_me
       , count(distinct message_id) as n_messages
       , count(case when is_text = true then message_id else null end) as n_text_messages
       , count(case when is_group_chat = true then message_id else null end) as n_messages_group_chat
       , count(case when is_text = true and is_group_chat = true then message_id else null end) as n_text_messages_group_chat
       , count(case when service = 'iMessage' then message_id else null end) as n_imessage
       , count(case when service = 'SMS' then message_id else null end) as n_sms
       , count(case when is_emote = true then message_id else null end) as n_emote
       , count(case when message_special_type = 'emote_remove_question' then message_id else null end) as n_emote_remove_question
       , count(case when message_special_type = 'emote_remove_like' then message_id else null end) as n_emote_remove_like
       , count(case when message_special_type = 'emote_remove_laugh' then message_id else null end) as n_emote_remove_laugh
       , count(case when message_special_type = 'emote_remove_heart' then message_id else null end) as n_emote_remove_heart
       , count(case when message_special_type = 'emote_remove_emphasis' then message_id else null end) as n_emote_remove_emphasis
       , count(case when message_special_type = 'emote_remove_dislike' then message_id else null end) as n_emote_remove_dislike
       , count(case when message_special_type = 'emote_question' then message_id else null end) as n_emote_question
       , count(case when message_special_type = 'emote_love' then message_id else null end) as n_emote_love
       , count(case when message_special_type = 'emote_like' then message_id else null end) as n_emote_like
       , count(case when message_special_type = 'emote_laugh' then message_id else null end) as n_emote_laugh
       , count(case when message_special_type = 'emote_emphasis' then message_id else null end) as n_emote_emphasis
       , count(case when message_special_type = 'emote_dislike' then message_id else null end) as n_emote_dislike
       , count(case when is_url = true then message_id else null end) as n_url
       , count(case when message_special_type is not null and is_emote = false then message_id else null end) as n_app_for_imessage
       , count(case when is_thread_origin = true then message_id else null end) as n_thread_origin
       , count(case when is_threaded_reply = true then message_id else null end) as n_threaded_reply
       , count(case when has_attachment = true then message_id else null end) as n_attachment
       , count(distinct contact_name) as n_contacts_messaged
from {pg_schema}.message_vw
group by dt, contact_name, is_from_me
