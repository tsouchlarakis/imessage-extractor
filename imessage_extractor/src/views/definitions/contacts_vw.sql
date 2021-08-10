drop view if exists {pg_schema}.contacts_vw;
create view {pg_schema}.contacts_vw as

select chat_identifier
       , contact_name
       , source
from (
    select chat_identifier,
        contact_name,
        source,
        row_number() over (partition by chat_identifier order by priority asc) as rank
    from (
        select chat_identifier, group_name as contact_name, 1 as priority, 'contact_group_names' as source
        from {pg_schema}.contact_group_names

        union

        select chat_identifier, contact_name, 2 as priority, 'contacts_manual' as source
        from {pg_schema}.contacts_manual

        union

        select chat_identifier, contact_name, 3 as priority, 'contacts' as source
        from {pg_schema}.contacts
    ) t1
) t2
where rank = 1
order by contact_name
