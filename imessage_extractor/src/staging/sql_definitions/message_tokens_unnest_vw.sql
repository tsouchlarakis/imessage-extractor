drop view if exists message_tokens_unnest_vw;
create view message_tokens_unnest_vw as

with message_user_candidates as (
    select message_id
           , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(text, '‘', ''''), '’', ''''), '“', '"'), '”', '"'), "'s", " 's"), "'d", " 'd"), '.', ' . '), ',', ' , '), '!', ' ! '), '?', ' ? ') as text
    from message_user
    where is_text = true
      and has_no_text = false
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

, split_with_stopwords as (
    select message_id
           , trim(token) as token
           , case when stopwords.stopword is not null then 1 else 0 end as is_stopword
    from split
    left join stopwords
      on lower(trim(split.token)) = stopwords.stopword
	where token != ''
)

select message_id
       , row_number() over(partition by message_id) as ordinal_position
       , token
       , is_stopword
from split_with_stopwords
where token != ''
