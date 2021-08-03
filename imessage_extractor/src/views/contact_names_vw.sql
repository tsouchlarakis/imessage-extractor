drop view if exists imessage.contact_token_usage_vw;

create or replace view imessage.contact_names_vw as

select
   r2.chat_identifier
   , r2.contact_name
from
(
   select
      contact_names.chat_identifier
      , contact_names.contact_name
   from
      imessage.contact_names
   union
   select
      contact_names_manual.chat_identifier
      , contact_names_manual.contact_name
   from imessage.contact_names_manual
   union
   select
      r1.cache_roomnames as chat_identifier
      , r1.group_title as contact_name
   from
   (
      select distinct
         message.cache_roomnames
         , message.group_title
         , row_number() over (partition by message.cache_roomnames order by message.date desc) as r
      from imessage_current.message
      where message.cache_roomnames is not null and message.group_title is not null
   ) r1
   where
      r1.r = 1
) r2
where
   r2.chat_identifier is not null