drop view if exists {pg_schema}.qc_missing_contact_names;
create or replace view {pg_schema}.qc_missing_contact_names as

select chat_identifier
       , message_date
       , "text"
from (
    select m.chat_identifier
           , m.message_date
           , m."text"
           , row_number() over(partition by m.chat_identifier order by message_date desc) as r
    from {pg_schema}.message_vw m
    left join {pg_schema}.contacts_ignored i
           on m.chat_identifier = i.chat_identifier
    where m.message_date > current_date - 35
      and i.chat_identifier is null
      and m.contact_name is null
      and m.is_text = true
      and not m.chat_identifier ~ '^\d{{5,6}}$'  -- Phone number of 5-6 digits is typically automated
) t
where r in (1, 2, 3)
order by chat_identifier, r
