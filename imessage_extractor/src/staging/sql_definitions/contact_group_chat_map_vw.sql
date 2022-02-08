drop view if exists contact_group_chat_map_vw;
create view contact_group_chat_map_vw as

select distinct contact_name, is_group_chat
from message_user
where contact_name is not null
