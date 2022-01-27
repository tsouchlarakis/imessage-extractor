drop view if exists message_tokens_vw;
create view message_tokens_vw as

select message_id,
       string_to_array(
         trim(
           regexp_replace(
             replace(
               replace(
                 regexp_replace(
                   replace(replace(replace(replace("text", '‘', ''''), '’', ''''), '“', '"'), '”', '"')  -- smart quotes
                   , '([\?\.\!,]+)(\M|$| )', ' \1 ', 'g'
                 )
                 , '''s', ' ''s'
               )
               , '''d', ' ''d'
             )
             , '\s+', ' ', 'g'
           )
         )
         , ' '
       ) as tokens
from message_user
where is_text = true
  and is_empty = false