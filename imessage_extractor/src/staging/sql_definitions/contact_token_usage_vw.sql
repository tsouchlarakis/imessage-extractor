drop view if exists contact_token_usage_vw;
create view contact_token_usage_vw as

select contact_name
       , "token"
       , "length"
       , sum(usages) as usages
from contact_token_usage_daily_from_who_vw
group by contact_name
         , "token"
         , "length"
