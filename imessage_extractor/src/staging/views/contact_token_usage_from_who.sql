drop view if exists {pg_schema}.contact_token_usage_from_who;
create or replace view {pg_schema}.contact_token_usage_from_who as

select contact_name
       , is_from_me
       , "token"
       , "length"
       , sum(usages) as usages
from {pg_schema}.contact_token_usage_daily_from_who
group by contact_name
         , is_from_me
         , "token"
         , "length"
