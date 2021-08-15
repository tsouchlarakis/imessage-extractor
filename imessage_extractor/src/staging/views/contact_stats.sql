drop view if exists {pg_schema}.contact_stats;
create or replace view {pg_schema}.contact_stats as

select m.contact_name
       , m.is_from_me
       , count(*) as messages
       , sum(m.n_characters) as characters
       , sum(m.n_tokens) as tokens
       , sum(case when m.service = 'iMessage' then 1 else 0 end) as imessages
       , sum(case when m.service = 'SMS' then 1 else 0 end) as sms
       , sum(case when m.is_text = true then 1 else 0 end) as messages_containing_text
       , sum(case when e.has_emoji = true then 1 else 0 end) as messages_containing_emoji
       , sum(case when m.is_emote = true then 1 else 0 end) as emotes
       , sum(case when m.is_url = true then 1 else 0 end) as urls
       , sum(case when m.is_thread = true then 1 else 0 end) as threaded_messages
       , sum(case when m.has_attachment = true then 1 else 0 end) as attachments
       , min(m.dt) as first_message_dt
       , max(m.dt) as latest_message_dt
       , current_date - max(m.dt) as days_since_latest_message
       , max(m.dt) - min(dt) as lifetime_days
       , count(distinct m.dt) as active_days
from {pg_schema}.message_vw m
left join {pg_schema}.message_emoji_map e
  on m.message_id = e.message_id
group by m.contact_name, m.is_from_me
order by m.contact_name, m.is_from_me
