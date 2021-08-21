drop view if exists {pg_schema}.message_emoji_map;
create or replace view {pg_schema}.message_emoji_map as

select m.message_id, coalesce(e.has_emoji, false) as has_emoji
from {pg_schema}.message_vw m
left join (
	select distinct t.message_id, true as has_emoji
	from {pg_schema}.message_tokens t
	join {pg_schema}.emoji_text_map e
	  on t."token" = e.emoji
) e on m.message_id = e.message_id
order by m.message_id desc
