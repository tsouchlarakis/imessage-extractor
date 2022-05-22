drop view if exists qc_message_special_types;
create view qc_message_special_types as

select associated_message_type
       , message_special_type as mapped_message_special_type_in_message_user
       , count(message_id) as n_messages
from message_user
where message_special_type is null
  and associated_message_type != 0
group by associated_message_type, message_special_type
order by count(*) desc
