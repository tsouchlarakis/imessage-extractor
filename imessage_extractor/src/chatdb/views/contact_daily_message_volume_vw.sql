drop view if exists {pg_schema}.contact_daily_message_volume_vw;
create or replace view {pg_schema}.contact_daily_message_volume_vw as

with messages as (
       select m.*, t.n_characters, t.n_tokens
       from {pg_schema}.message_vw m
       left join {pg_schema}.message_vw_text t
         on m.message_id = t.message_id
)

select contact_name
       , dt
       , n_total_messages
       , n_text_messages
       , n_emote
       , n_url
       , n_text_characters
       , n_text_words
       , day_rank_by_n_messages_partition_by_contact
from (
    select contact_name
           , dt
           , is_from_me
           , count(distinct message_id) as n_total_messages
           , count(distinct case when is_text = true then message_id end) as n_text_messages
           , count(distinct case when is_emote = true then message_id end) as n_emote
           , count(distinct case when is_url = true then message_id end) as n_url
           , sum(n_characters) as n_text_characters
           , sum(n_tokens) as n_text_words
           , row_number() over(partition by contact_name order by count(distinct message_id) desc) as day_rank_by_n_messages_partition_by_contact
    from messages
    where is_text = true
      and contact_name is not null
    group by contact_name
             , dt
             , is_from_me
) t1
order by contact_name
         , dt asc
         , is_from_me
