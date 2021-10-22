drop view if exists {pg_schema}.contact_token_usage;
create or replace view {pg_schema}.contact_token_usage as

select contact_name
       , "token"
       , "length"
       , sum(usages) as usages
from {pg_schema}.contact_token_usage_daily_from_who
group by contact_name
         , "token"
         , "length"
