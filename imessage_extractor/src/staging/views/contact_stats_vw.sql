drop view if exists contact_stats_vw;
create view contact_stats_vw as

select m.contact_name
       , m.is_from_me
       , count(*) as messages
       , sum(mt.n_characters) as characters
       , sum(mt.n_tokens) as tokens
       , sum(case when m.service = 'iMessage' then 1 else 0 end) as imessages
       , sum(case when m.service = 'SMS' then 1 else 0 end) as sms
       , sum(case when m.is_text = true then 1 else 0 end) as messages_containing_text
       , sum(case when m.is_emote = true then 1 else 0 end) as emotes
       , sum(case when m.is_url = true then 1 else 0 end) as urls
       , sum(case when m.is_thread_origin = true then 1 else 0 end) as thread_origins
       , sum(case when m.is_threaded_reply = true then 1 else 0 end) as threaded_replies
       , sum(case when m.has_attachment = true then 1 else 0 end) as messages_containing_attachments
       , min(m.dt) as first_message_dt
       , max(m.dt) as latest_message_dt
       , current_date - max(m.dt) as days_since_latest_message
       , max(m.dt) - min(m.dt) as lifetime_days
       , count(distinct m.dt) as active_days
from message_user m
left join message_user_text_vw mt on m.message_id = mt.message_id
group by m.contact_name, m.is_from_me
order by m.contact_name, m.is_from_me
