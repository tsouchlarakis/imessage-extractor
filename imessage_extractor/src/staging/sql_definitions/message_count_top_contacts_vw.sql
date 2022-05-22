drop view if exists message_count_top_contacts_vw;
create view message_count_top_contacts_vw as

select contact_name
       , count(distinct message_id) as n_messages
from message_user
where contact_name is not null
group by contact_name
order by count(distinct message_id) desc
limit 100
