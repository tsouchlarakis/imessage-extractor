drop view if exists qc_null_flags;
create view qc_null_flags as

select *
from message_user
where is_from_me is null
   or is_text is null
   or is_url is null
   or is_group_chat is null
   or is_emote is null
   or is_empty is null
order by message_id desc
