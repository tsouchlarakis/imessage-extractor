drop view if exists imessage.contact_token_usage_vw;

create or replace view imessage.contact_token_usage_vw as

with t1 as (
    select
        mt.message_uid
        , m.contact_name
        , m.message_date :: date as message_date
        , m.is_from_me
        , lower(mt."token") as "token"
        , t.lemma
        , t.stem
        , t."length"
        , mt.pos
        , mt.pos_simple
        , t."language"
    from
        imessage.message_tokens mt
        join imessage.tokens t
          on lower(t."token") = lower(mt."token")
        join imessage.message_vw m
          on m.message_uid = mt.message_uid
    where
        t.is_punct = false
        and t.is_english_stopword = false
        and m.contact_name is not null
        and mt.message_uid is not null
        and is_text = true
)

select
    contact_name
    , is_from_me
    , "token"
    , "language"
    , count(*) as n_token_uses
    , row_number() over(partition by contact_name order by count(*) desc) as token_rank_within_contact
    , count(distinct message_uid) as n_messages_where_token_used
    , min(message_date) as first_use_date
    , max(message_date) as last_use_date
from
    t1
group by
    contact_name
    , is_from_me
    , "token"
    , "language"
having
    count(*) > 1
order by
    contact_name
    , count(*) desc
