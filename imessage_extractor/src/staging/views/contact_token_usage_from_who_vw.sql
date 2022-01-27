drop view if exists contact_token_usage_from_who_vw;
create view contact_token_usage_from_who_vw as

select contact_name
       , is_from_me
       , "token"
       , "length"
       , sum(usages) as usages
from contact_token_usage_daily_from_who_vw
group by contact_name
         , is_from_me
         , "token"
         , "length"
