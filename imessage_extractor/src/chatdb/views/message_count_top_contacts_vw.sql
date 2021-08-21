drop view if exists {pg_schema}.message_count_top_contacts_vw;
create or replace view {pg_schema}.message_count_top_contacts_vw as

select contact_name
       , count(distinct message_id) as n_messages
from {pg_schema}.message_vw
where contact_name is not null
group by contact_name
order by count(distinct message_id) desc
limit 100
