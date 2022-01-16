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
           , m.associated__type as associated_type
           , m.balloon_bundle_id
           , m."service"
           , case when m.is_from_me = 1 then true when m.is_from_me = 0 then false else null end as is_from_me
           , coalesce(thread_origins.is_thread_origin, false) as is_thread_origin
           , case when m.thread_originator_guid is not null then true else false end as is_threaded_reply
           , threaded_replies.thread_original_message_id
           , case when m.cache_has_attachments = 1 then true
                  when m.cache_has_attachments = 0 then false
                  else null
             end as has_attachment
           , m."attributedBody" as attributed_body
           , m.was_data_detected
           , m.cache_has_attachments
    from {pg_schema}.message m
    left join (
        -- Get the ROWID for all messages that have a thread_originator_guid
        select distinct t1.thread_originator_guid, t2."ROWID" as thread_original_message_id
        from {pg_schema}.message t1
        join {pg_schema}.message t2
          on t1.thread_originator_guid = t2.guid
    ) as threaded_replies
      on m.thread_originator_guid = threaded_replies.thread_originator_guid
    left join (
        -- Get a boolean flag for all messages that are the origin of a thread. These messages
        -- will not have a thread_originator_guid, because at the time that they are sent,
        -- they are not yet part of a thread
        select "ROWID" as thread_originator_guid, true as is_thread_origin
        from {pg_schema}.message
        where thread_originator_guid is not null
    ) as thread_origins
      on m."ROWID" = thread_origins.thread_originator_guid
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
           , case when m.associated_type in (2000, 2001, 2002, 2003, 2004, 2005, 3000, 3001, 3002, 3003, 3004, 3005) then true
                  else false
             end as is_emote
           , case when m.associated_type = 2 and m.balloon_bundle_id ilike '%PeerPaymentMessagesExtension' then 'apple_cash'
                  when m.associated_type = 2 and m.balloon_bundle_id ilike '%imessagepoll%' then 'poll'
                  when m.associated_type in (2, 3) and m.balloon_bundle_id ilike '%gamepigeon%' then 'game_pigeon'
                  when m.associated_type in (2, 3) and m.balloon_bundle_id ilike '%messages.business.extension%' then 'business_extension'
                  when m.associated_type = 3 and m."text" ilike '%earned an achievement.' then 'activity'
                  when m.associated_type = 3 and m."text" ilike '%completed a workout.' then 'activity'
                  when m.associated_type = 3 and m."text" ilike '%closed all three Activity rings.' then 'activity'
                  when m.associated_type = 3 and m."text" ilike 'Requested % with Apple Pay.' then 'apple_cash'
                  when m.associated_type = 3 and (m."text" ilike '%poll%' or m."text" ilike '%voted%') then 'poll'
                  when m.associated_type = 3 and m."text" = 'Cup Pong' then 'game_pigeon'
                  when m.associated_type = 3 and m."text" = '8 Ball' then 'game_pigeon'
                  when m.associated_type = 3 and m."text" = '(null)' then 'null_message'
                  when m.associated_type = 1000 and m.cache_has_attachments = 1 and m.was_data_detected = 1 and m.attributed_body ilike '%kIMFileTransferGUIDAttributeName%' then 'sticker'
                  when m.associated_type = 2000 then 'emote_love'
                  when m.associated_type = 2001 then 'emote_like'
                  when m.associated_type = 2002 then 'emote_dislike'
                  when m.associated_type = 2003 then 'emote_laugh'
                  when m.associated_type = 2004 then 'emote_emphasis'
                  when m.associated_type = 2005 then 'emote_question'
                  when m.associated_type = 3000 then 'emote_remove_heart'
                  when m.associated_type = 3001 then 'emote_remove_like'
                  when m.associated_type = 3002 then 'emote_remove_dislike'
                  when m.associated_type = 3003 then 'emote_remove_laugh'
                  when m.associated_type = 3004 then 'emote_remove_emphasis'
                  when m.associated_type = 3005 then 'emote_remove_question'
                  else null
             end as message_special_type
           , associated_type
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
           , is_thread_origin
           , is_threaded_reply
           , thread_original_message_id
           , has_attachment
    from {pg_schema}.chat c
    join (
    	select chat_id, message_id
      from (
        -- Use a window function to avoid one-to-many message_id: chat_id mappings
        select chat_id
               , message_id
               , row_number() over(partition by message_id order by message_date desc) as r
        from {pg_schema}.chat_message_join
      ) cm_join
      where r = 1
    ) cm_mapping on c."ROWID" = cm_mapping.chat_id
    join m
      on cm_mapping.message_id = m.message_id
    left join {pg_schema}.contacts_vw n
      on c._identifier = n.chat_identifier
)

select message_id
       , chat_identifier
       , contact_name
       , ts
       , dt
       , case when "text" = '' then null else "text" end as "text"
       , service
       , is_from_me
       , is_group_chat
       , case when is_emote = false and is_url = false and message_special_type is null
                   then true
              else false
         end as is_text
       , is_empty
       , is_emote
       , message_special_type
       , associated_type
       , is_url
       , is_thread_origin
       , is_threaded_reply
       , thread_original_message_id
       , has_attachment
from m_join_chat_contacts
order by message_id desc nulls last
