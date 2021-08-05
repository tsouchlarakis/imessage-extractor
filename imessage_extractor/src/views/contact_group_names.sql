drop view if exists imessage_test.contact_group_names;
create view imessage_test.contact_group_names as

select r1.cache_roomnames as chat_identifier, r1.group_title as group_name
from (
    select distinct
        cache_roomnames,
        group_title,
        row_number() over (partition by cache_roomnames order by "date" desc) as r
  from imessage_test.message
  where message.cache_roomnames is not null
    and message.group_title is not null
) r1
where r1.r = 1
