drop view if exists {pg_schema}.message_vw;
create view {pg_schema}.message_vw as

with m as (
       select m."ROWID" as message_id
              , to_timestamp(((m."date"::double precision / 1000000000::double precision)::numeric + '978307200'::numeric)::double precision) as ts
              , btrim(
                  regexp_replace(
                      replace(
                           -- NOTE: curly braces are doubled to comply for python format string
                           regexp_replace(m."text", '\.{{2,}}', '…', 'g'),
                           '￼', ''
                      ),
                      '[\n\r]+', ' ', 'g'
                  )
              ) as "text"
              , m.associated__type
              , m."service"
              , case when m.is_from_me = 1 then true when m.is_from_me = 0 then false else null end as is_from_me
              , case when m.thread_originator_guid is not null then true else false end as is_thread
              , thread_origins.thread_original_message_id
              , case when m.cache_has_attachments = 1 then true when m.cache_has_attachments = 0 then false else null end as has_attachment
       from {pg_schema}.message m
       left join (
           select distinct t1.thread_originator_guid, t2."ROWID" as thread_original_message_id
           from {pg_schema}.message t1
           join {pg_schema}.message t2
             on t1.thread_originator_guid = t2.guid
       ) as thread_origins
       on m.thread_originator_guid = thread_origins.thread_originator_guid
),

m_join_chat_contacts as (
       select m.message_id
              , c._identifier as chat_identifier
              , n.contact_name
              , m.ts
              , m.ts :: date as dt
              , "text"
              , m.service
              , m.is_from_me
              , case when m.associated__type in (2000, 2001, 2002, 2003, 2004, 2005, 3000, 3001, 3002, 3003, 3004, 3005) then true
                     else false
                end as is_emote
              , case when m."text" ilike '$%completed a workout.' then 'workout_notification'
                     when m."text" ilike '$%closed all three Activity rings.' then 'workout_notification'
                     when m."text" ilike '$%earned an achievement.' then 'workout_notification'
                     when m.associated__type = 3 then 'app_for_imessage'
                     when m.associated__type = 2000 then 'love'
                     when m.associated__type = 2001 then 'like'
                     when m.associated__type = 2002 then 'dislike'
                     when m.associated__type = 2003 then 'laugh'
                     when m.associated__type = 2004 then 'emphasis'
                     when m.associated__type = 2005 then 'question'
                     when m.associated__type = 3000 then 'remove_heart'
                     when m.associated__type = 3001 then 'remove_like'
                     when m.associated__type = 3002 then 'remove_dislike'
                     when m.associated__type = 3003 then 'remove_laugh'
                     when m.associated__type = 3004 then 'remove_emphasis'
                     when m.associated__type = 3005 then 'remove_question'
                     else null
                end as message_special_type
              , case when c._identifier ilike 'chat%' then true
                     else false
                end as is_group_chat
              , case when btrim(regexp_replace(m."text", '[\n\r]+', ' ', 'g')) ilike 'http:%'
                       or btrim(regexp_replace(m."text", '[\n\r]+', ' ', 'g')) ilike 'https:%'
                     then true
                     else false
                end as is_url
              , case when length(btrim(replace(regexp_replace(m."text", '[\n\r]+', ' ', 'g'), '￼', ''))) = 0 then true
                     when m."text" is null then true
                     else false
                end as is_empty
              , is_thread
              , thread_original_message_id
              , has_attachment
       from {pg_schema}.chat c
       join {pg_schema}.chat_message_join cm_join
         on c."ROWID" = cm_join.chat_id
       join m
         on cm_join.message_id = m.message_id
       left join {pg_schema}.contacts_vw n
         on c._identifier = n.chat_identifier
)

select message_id
       , chat_identifier
       , contact_name
       , ts
       , dt
       , case when "text" = '' then null else "text" end as "text"
       , length(case when is_emote = false and is_url = false and is_empty = false and message_special_type is null
                          then "text"
                     else null
                end) as n_characters
       , case when is_emote = false and is_url = false and message_special_type is null
                   then array_length(regexp_split_to_array("text", '\s+'), 1)
              when is_empty
                   then 0
              else null
         end as n_tokens
       , service
       , is_from_me
       , is_group_chat
       , case when is_emote = false and is_url = false and is_empty = false and message_special_type is null
                   then true
              else false
         end as is_text
       , is_empty
       , is_emote
       , message_special_type
       , is_url
       , is_thread
       , thread_original_message_id
       , has_attachment
from m_join_chat_contacts
order by message_id desc nulls last
