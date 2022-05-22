drop view if exists qc_duplicate_message_id;
create view qc_duplicate_message_id as

select message_id, count(*) as n_records
from message_user
group by message_id
having count(*) > 1
