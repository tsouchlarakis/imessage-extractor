drop view if exists {pg_schema}.qc_message_special_types;
create or replace view {pg_schema}.qc_message_special_types as

select associated_type, message_special_type, count(*) as n
from {pg_schema}.message_vw
where message_special_type is null
  and associated_type != 0
group by associated_type, message_special_type
order by count(*) desc
