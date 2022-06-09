drop view if exists qc_message_special_types;
create view qc_message_special_types as

select m.associated_message_type
       , mu.message_special_type as mapped_message_special_type_in_message_user
       , count(mu.message_id) as n_messages
from message_user mu
join message m
  on mu.message_id = m.ROWID
where mu.message_special_type is null
  and m.associated_message_type != 0
group by m.associated_message_type, mu.message_special_type
order by count(*) desc
