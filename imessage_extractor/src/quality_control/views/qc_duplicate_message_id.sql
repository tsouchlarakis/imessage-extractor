drop view if exists {pg_schema}.qc_duplicate_message_id;
create or replace view {pg_schema}.qc_duplicate_message_id as

select message_id, count(*) as n_records
from {pg_schema}.message_vw
group by message_id
having count(*) > 1
