drop view if exists {pg_schema}.contact_token_usage_daily_from_who;
create or replace view {pg_schema}.contact_token_usage_daily_from_who as

select m.contact_name
       , m.dt
       , m.is_from_me
       , u."token"
       , length(u."token") as length
       , count(distinct m.message_id) as usages
from {pg_schema}.message_vw_text m
join {pg_schema}.message_tokens_unnest u
  on m.message_id = u.message_id
where length(u."token") > 0
group by m.contact_name, m.dt, m.is_from_me, u."token"
order by m.contact_name, m.dt, m.is_from_me, u."token"
