drop view if exists qc_missing_contact_names;
create view qc_missing_contact_names as

with total_messages_by_contact as (
    select chat_identifier,
           count(case when is_text = true then message_id else null end) as total_messages
    from message_user
    group by chat_identifier
),

text_messages_mising_contact_identifiers as (
    select m.chat_identifier,
           m.dt,
           m."text",
           row_number() over(partition by m.chat_identifier order by dt desc) as message_recency_row_number
    from message_user m
    left join contacts_ignored i
      on m.chat_identifier = i.chat_identifier
    where i.chat_identifier is null
      and m.contact_name is null
      and m.is_text = true
)

select m.chat_identifier,
       m.dt,
       m."text",
       t.total_messages
from text_messages_mising_contact_identifiers m
left join total_messages_by_contact t
  on m.chat_identifier = t.chat_identifier
where message_recency_row_number <= 3  -- Only grab the three most recent dates so the final query result isn't too cluttered
  and t.total_messages > 10  -- Only show chats with more than X total messages
order by total_messages desc, m.chat_identifier
