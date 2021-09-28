drop view if exists {pg_schema}.daily_summary_from_who;
create or replace view {pg_schema}.daily_summary_from_who as

select dt
       , is_from_me
       , sum(n_messages) as n_messages
       , sum(n_text_messages) as n_text_messages
       , sum(n_messages_group_chat) as n_messages_group_chat
       , sum(n_text_messages_group_chat) as n_text_messages_group_chat
       , sum(n_imessage) as n_imessage
       , sum(n_sms) as n_sms
       , sum(n_emote) as n_emote
       , sum(n_emote_remove_question) as n_emote_remove_question
       , sum(n_emote_remove_like) as n_emote_remove_like
       , sum(n_emote_remove_laugh) as n_emote_remove_laugh
       , sum(n_emote_remove_heart) as n_emote_remove_heart
       , sum(n_emote_remove_emphasis) as n_emote_remove_emphasis
       , sum(n_emote_remove_dislike) as n_emote_remove_dislike
       , sum(n_emote_question) as n_emote_question
       , sum(n_emote_love) as n_emote_love
       , sum(n_emote_like) as n_emote_like
       , sum(n_emote_laugh) as n_emote_laugh
       , sum(n_emote_emphasis) as n_emote_emphasis
       , sum(n_emote_dislike) as n_emote_dislike
       , sum(n_url) as n_url
       , sum(n_app_for_imessage) as n_app_for_imessage
       , sum(n_thread_origin) as n_thread_origin
       , sum(n_threaded_reply) as n_threaded_reply
       , sum(n_attachment) as n_attachment
       , sum(n_contacts_messaged) as n_contacts_messaged
from {pg_schema}.daily_summary_contact_from_who
group by dt, is_from_me
