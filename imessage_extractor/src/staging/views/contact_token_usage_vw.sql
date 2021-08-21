drop view if exists {pg_schema}.contact_token_usage_vw;
create or replace view {pg_schema}.contact_token_usage_vw as

with t1 as (
    select mt.message_id
           , m.contact_name
           , m.dt
           , m.is_from_me
           , lower(mt."token") as "token"
           , t.is_emoji
           , t.lemma
           , t.stem
           , t."length"
           , mt.pos
           , mt.pos_simple
    from {pg_schema}.message_tokens mt
    join {pg_schema}.tokens t
      on lower(t."token") = lower(mt."token")
    join {pg_schema}.message_vw m
      on m.message_id = mt.message_id
    where t.is_punct = false
      and t.is_english_stopword = false
      and m.contact_name is not null
      and mt.message_id is not null
      and is_text = true
)

select contact_name
       , "token"
       , is_from_me
       , is_emoji
       , count(*) as n_token_uses
       , row_number() over(partition by contact_name order by count(*) desc) as token_rank_within_contact
       , count(distinct message_id) as n_messages_where_token_used
       , min(dt) as first_use_dt
       , max(dt) as last_use_dt
from t1
group by contact_name
         , "token"
         , is_from_me
         , is_emoji
having count(*) > 1
order by contact_name , count(*) desc
