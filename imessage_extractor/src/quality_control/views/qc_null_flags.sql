drop view if exists {pg_schema}.qc_null_flags;
create or replace view {pg_schema}.qc_null_flags as

select *
from {pg_schema}.message_vw
where is_from_me is null
   or is_text is null
   or is_url is null
   or is_group_chat is null
   or is_emote is null
   or is_empty is null
order by message_id desc
