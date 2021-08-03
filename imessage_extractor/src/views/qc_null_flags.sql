drop view if exists imessage.qc_null_flags;

create or replace view imessage.qc_null_flags as

select
   *
from
   imessage.message_vw
where
   is_from_me is null
   or is_text is null
   or is_url is null
   or is_group_chat is null
   or is_emote is null
   or is_empty is null