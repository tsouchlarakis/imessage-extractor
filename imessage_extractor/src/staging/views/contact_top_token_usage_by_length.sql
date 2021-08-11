drop view if exists {pg_schema}.contact_top_token_usage_by_length;
create or replace view {pg_schema}.contact_top_token_usage_by_length as

select contact_name
       , is_from_me
       , token
       , token_length
       , n_token_uses
from (
    select contact_name
           , is_from_me
           , token
           , length(token) as token_length
           , n_token_uses
           , row_number() over(partition by contact_name, is_from_me, length(token) order by n_token_uses desc) as r
    from {pg_schema}.contact_token_usage_vw
) t1
where r = 1
order by contact_name
         , is_from_me
         , token_length asc
