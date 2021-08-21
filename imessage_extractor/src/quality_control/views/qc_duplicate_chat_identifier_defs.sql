/*
Find chat identifiers, if any, that are defined in any two or more
of the following source:

  1. Contact group names view (based on the `group_title`) column
     of the 'message' chat.db table
  2. Manually defined contacts in the contacts_manual.csv table.
  3. Contacts table, generally the output of a contacts exporter
     app, that is then uploaded to Postgres.

For example, if the same contact is in your contacts AND in the
manual contacts table, one must be chosen to represent that
`chat_identifier`, so there must be a way to disambiguate.
That method of disambiguating is represented by the `priority`
column below. As such, these conflicts are able to be handled,
but ideally none would exist.

Use this query to identify duplicate contact chat identifiers,
then resolve them by making sure they appear in only one of
the preceding three sources.
*/

drop view if exists {pg_schema}.qc_duplicate_chat_identifier_defs;
create or replace view {pg_schema}.qc_duplicate_chat_identifier_defs as

with all_contacts as (
    select chat_identifier, group_name as contact_name, 1 as priority, 'contact_group_names' as source
    from imessage_test.contact_group_names

    union

    select chat_identifier, contact_name, 2 as priority, 'contacts_manual' as source
    from imessage_test.contacts_manual

    union

    select chat_identifier, contact_name, 3 as priority, 'contacts' as source
    from imessage_test.contacts
),

duplicate_contacts as (
    select chat_identifier, count(*) as n
    from all_contacts
    group by chat_identifier
    having count(*) > 1
)

select ac.chat_identifier, ac.contact_name, ac.priority, ac.source
from all_contacts ac
join duplicate_contacts dc
  on ac.chat_identifier = dc.chat_identifier
order by chat_identifier, priority
