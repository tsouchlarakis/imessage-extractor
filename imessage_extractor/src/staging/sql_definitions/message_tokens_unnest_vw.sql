drop view if exists message_tokens_unnest_vw;
create view message_tokens_unnest_vw as

with message_user_candidates as (
    select message_id
           , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(text, '‘', ''''), '’', ''''), '“', '"'), '”', '"'), "'s", " 's"), "'d", " 'd"), '.', ' . '), ',', ' , '), '!', ' ! '), '?', ' ? ') as text
    from message_user
    where is_text = true
      and is_empty = false
)

, split(message_id, token, str) as (
    select message_id, '', text||' '
    from message_user_candidates

    union all

    select message_id,
           substr(str, 0, instr(str, ' ')),
           substr(str, instr(str, ' ') + 1)
    from split
    where str != ''
)

select message_id, token
from split
where token != ''
