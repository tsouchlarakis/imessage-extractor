drop view if exists {pg_schema}.message_tokens_unnest;
create or replace view {pg_schema}.message_tokens_unnest as

with unnested_tokens as (
    select message_id, unnest(tokens) as token
    from {pg_schema}.message_tokens
)

select ut.message_id
       , ut.token
       , case when e.emoji is not null then true else false end as is_emoji
from unnested_tokens ut
left join {pg_schema}.emoji_text_map e
  on e.emoji = ut.token