drop view if exists message_emoji_map_vw;
create view message_emoji_map_vw as

select message_id, max(has_emoji) as has_emoji
from (
	select m.message_id, case when e.emoji is not null then true else false end as has_emoji
	from message_user m
	left join message_tokens_unnest_vw u
	  on m.message_id = u.message_id
	left join emoji_text_map e
	  on u.token = e.emoji
) as t
group by message_id
