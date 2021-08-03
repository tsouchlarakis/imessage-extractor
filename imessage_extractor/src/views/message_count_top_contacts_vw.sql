drop view if exists imessage.message_count_top_contacts_vw;

create or replace view imessage.message_count_top_contacts_vw as

select
    contact_name
    , count(distinct message_uid) as n_messages
from
    imessage.message_vw
where
    contact_name is not null
group by
    contact_name
order by
    count(distinct message_uid) desc
limit
    100