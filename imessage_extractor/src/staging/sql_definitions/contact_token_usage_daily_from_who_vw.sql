drop view if exists contact_token_usage_daily_from_who_vw;
create view contact_token_usage_daily_from_who_vw as

select m.contact_name
       , m.dt
       , m.is_from_me
       , u."token"
       , length(u."token") as length
       , count(distinct m.message_id) as usages
from message_user_text_vw m
join message_tokens_unnest_vw u
  on m.message_id = u.message_id
where length(u."token") > 0
group by m.contact_name, m.dt, m.is_from_me, u."token"
order by m.contact_name, m.dt, m.is_from_me, u."token"
