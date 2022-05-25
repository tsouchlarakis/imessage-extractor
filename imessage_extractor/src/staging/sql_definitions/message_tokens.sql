drop view if exists {pg_schema}.message_tokens;
create or replace view {pg_schema}.message_tokens as

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
from {pg_schema}.message_vw
where is_text = 1
  and has_no_text = 0