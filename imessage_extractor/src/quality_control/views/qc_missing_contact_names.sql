drop view if exists {pg_schema}.qc_missing_contact_names;
create or replace view {pg_schema}.qc_missing_contact_names as

select chat_identifier,
       dt,
       "text"
from (
    select m.chat_identifier,
           m.dt,
           m."text",
           row_number() over(partition by m.chat_identifier order by dt desc) as r
    from {pg_schema}.message_vw m
    left join {pg_schema}.contacts_ignored i
      on m.chat_identifier = i.chat_identifier
    where i.chat_identifier is null
      and m.contact_name is null
      and m.is_text = true
      and not m.chat_identifier ~ '^\d{{5,6}}$'  -- Phone number of 5-6 digits is typically automated
) t
where r in (1, 2, 3)
order by chat_identifier, r
