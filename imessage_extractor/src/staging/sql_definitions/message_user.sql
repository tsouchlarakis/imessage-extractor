drop table if exists message_user;
create table message_user as

-- Gather information about the types of messages sent and received
with message_types as (
    select ROWID
           , service
           , case when associated_message_type in (2000, 2001, 2002, 2003, 2004, 2005, 3000, 3001, 3002, 3003, 3004, 3005) then true
                  else false
             end as is_emote
           , case when text like 'http:%'
                    or text like 'https:%'
                  then true
                  else false
             end as is_url
           , case when balloon_bundle_id like '%PeerPaymentMessagesExtension' then 'apple_cash'
                  when associated_message_type = 2 and balloon_bundle_id like '%imessagepoll%' then 'poll'
                  when associated_message_type in (2, 3) and balloon_bundle_id like '%gamepigeon%' then 'game_pigeon'
                  when associated_message_type in (2, 3) and balloon_bundle_id like '%messages.business.extension%' then 'business_extension'
                  when associated_message_type = 3 and text like '%earned an achievement.' then 'activity'
                  when associated_message_type = 3 and text like '%completed a workout.' then 'activity'
                  when associated_message_type = 3 and text like '%closed all three Activity rings.' then 'activity'
                  when associated_message_type = 3 and text like 'Requested % with Apple Pay.' then 'apple_cash'
                  when associated_message_type = 3 and (text like '%poll%' or text like '%voted%') then 'poll'
                  when associated_message_type = 3 and text = 'Cup Pong' then 'game_pigeon'
                  when associated_message_type = 3 and text = '8 Ball' then 'game_pigeon'
                  when associated_message_type = 3 and text = '(null)' then 'null_message'
                  when associated_message_type = 1000 and cache_has_attachments = true and was_data_detected = true then 'sticker'
                  when associated_message_type = 2000 then 'emote_love'
                  when associated_message_type = 2001 then 'emote_like'
                  when associated_message_type = 2002 then 'emote_dislike'
                  when associated_message_type = 2003 then 'emote_laugh'
                  when associated_message_type = 2004 then 'emote_emphasis'
                  when associated_message_type = 2005 then 'emote_question'
                  when associated_message_type = 3000 then 'emote_remove_heart'
                  when associated_message_type = 3001 then 'emote_remove_like'
                  when associated_message_type = 3002 then 'emote_remove_dislike'
                  when associated_message_type = 3003 then 'emote_remove_laugh'
                  when associated_message_type = 3004 then 'emote_remove_emphasis'
                  when associated_message_type = 3005 then 'emote_remove_question'
                  else null
               end as message_special_type
    from message
)

-- Cleanly extract message text
, message_text as (
    select ROWID
           , case when text = '' then null
                  when text = '�' then null
                  when is_emote = true then null
                  when is_url = true then null
                  else text
            end as text
    from (
        select m.ROWID
               -- char(13) = carriage return, char(10) = line break
               , trim(replace(replace(replace(m.text, '￼', ''), char(13), ' '), char(10), ' ')) as text
               , mt.is_emote
               , mt.is_url
        from message m
        join message_types mt
          on m.ROWID = mt.ROWID
    ) t
)

-- Finalize information about the types of messages sent and received
, message_types2 as (
    select mt.ROWID
           , service
           , mt.message_special_type
           , mt.is_emote
           , mt.is_url
           , case when mt.is_emote = false
                    and mt.is_url = false
                    and mt.message_special_type is null
                    and tt.text is not null
                    then true
                  else false
             end as is_text
           , case when tt.text is null then true else false end as has_no_text
    from message_types mt
    join message_text tt
      on mt.ROWID = tt.ROWID
)

-- Attachment information by message
, attachments as (
    select m.ROWID
           , case when m.cache_has_attachments = true and mt.is_emote = false then true else false end as has_attachment
           , case when m.cache_has_attachments = true and mt.is_url = false and mt.is_text = false then true else false end as is_attachment
           , case when m.cache_has_attachments = true and mt.is_url = false and mt.message_special_type is null then true else false end as has_attachment_image
           , case when m.cache_has_attachments = true and mt.is_url = false and mt.message_special_type is null and mt.is_text = false then true else false end as is_attachment_image
    from message m
    join message_types2 mt
      on m.ROWID = mt.ROWID
)

-- Get the ROWID for all messages that have a thread_originator_guid and flags to
-- indicate whether the given message is the originator of a thread of a reply to
-- an existing thread
, threads as (
    select m1.ROWID
          , m2.ROWID as thread_original_message_id
          , case when m2.ROWID is not null then true else false end is_threaded_reply
          , coalesce(thread_origins.is_thread_origin, false) as is_thread_origin
    from message m1
    left join message m2
      on m1.thread_originator_guid = m2.guid
    left join (
      select distinct m1.ROWID as message_id, true as is_thread_origin
      from message m1
      join message m2
        on m1.guid = m2.thread_originator_guid
    ) thread_origins
      on m1.ROWID = thread_origins.message_id
)

-- Which contacts and group chat(s) (if any) each message is associated with
, contacts_and_group_chat_info as (
    select m.ROWID
           , c.chat_identifier
           , n.contact_name
           , case when c.chat_identifier like 'chat%' then true
                  else false
             end as is_group_chat
    from chat c
    join (select chat_id, message_id
          from (-- Use a window function to avoid one-to-many message_id: chat_id mappings
                select chat_id
                       , message_id
                       , row_number() over(partition by message_id order by message_date desc) as r
                from chat_message_join
          ) cm_join
          where r = 1
    ) cm_mapping
      on c.ROWID = cm_mapping.chat_id
    join message m
      on cm_mapping.message_id = m.ROWID
    left join contacts_user n
      on c.chat_identifier = n.chat_identifier
)

-- Message dates and times
, dates_and_times as (
    select sent_times.ROWID
           , sent_times.ts
           , sent_times.dt
           , sent_times.ts_utc
           , sent_times.dt_utc
           , read_by_them_times.ts_read_by_them
           , read_by_them_times.dt_read_by_them
           , read_by_them_times.ts_read_by_them_utc
           , read_by_them_times.dt_read_by_them_utc
           , read_by_them_times.elapsed_seconds_for_them_to_read
    from (
        select ROWID
               , ts
               , date(ts) as dt
               , ts_utc
               , date(ts_utc) as dt_utc
        from (
            select ROWID
                   , datetime(`date` / 1000000000 + 978307200, 'unixepoch', 'localtime') as ts
                   , datetime(`date` / 1000000000 + 978307200, 'unixepoch', 'utc') as ts_utc
            from message
        ) t1
    ) sent_times
    left join (
        select ROWID
               , ts_read_by_them
               , date(ts_read_by_them) as dt_read_by_them
               , ts_read_by_them_utc
               , date(ts_read_by_them_utc) as dt_read_by_them_utc
               , cast(round((julianday(ts_read_by_them) - julianday(ts)) * 86400) as int) as elapsed_seconds_for_them_to_read
        from (
            select m.ROWID
                   , datetime(m.`date` / 1000000000 + 978307200, 'unixepoch', 'localtime') as ts
                   , case when m.date_read != 0 then datetime(m.date_read / 1000000000 + 978307200, 'unixepoch', 'localtime') else null end as ts_read_by_them
                   , case when m.date_read != 0 then datetime(m.date_read / 1000000000 + 978307200, 'unixepoch', 'utc') else null end as ts_read_by_them_utc
            from message m
            join message_types2 mt
              on m.ROWID = mt.ROWID
            where m.is_from_me = true
              and mt.is_text = true
        ) r1
        where r1.ts_read_by_them > r1.ts
    ) read_by_them_times
      on sent_times.ROWID = read_by_them_times.ROWID
)

select message.ROWID as message_id
       , dates_and_times.ts
       , dates_and_times.dt
       , dates_and_times.ts_utc
       , dates_and_times.dt_utc
       , dates_and_times.ts_read_by_them
       , dates_and_times.dt_read_by_them
       , dates_and_times.ts_read_by_them_utc
       , dates_and_times.dt_read_by_them_utc
       , dates_and_times.elapsed_seconds_for_them_to_read
       , contacts_and_group_chat_info.chat_identifier
       , contacts_and_group_chat_info.contact_name
       , contacts_and_group_chat_info.is_group_chat
       , message_text.text
       , message.is_from_me
       , message_types2.service
       , message_types2.message_special_type
       , message_types2.is_emote
       , message_types2.is_url
       , message_types2.is_text
       , message_types2.has_no_text
       , attachments.has_attachment
       , attachments.is_attachment
       , attachments.has_attachment_image
       , attachments.is_attachment_image
       , threads.thread_original_message_id
       , threads.is_threaded_reply
       , threads.is_thread_origin
from message
left join message_types2
  on message.ROWID = message_types2.ROWID
left join message_text
  on message.ROWID = message_text.ROWID
left join attachments
  on message.ROWID = attachments.ROWID
left join threads
  on message.ROWID = threads.ROWID
left join contacts_and_group_chat_info
  on message.ROWID = contacts_and_group_chat_info.ROWID
left join dates_and_times
  on message.ROWID = dates_and_times.ROWID
order by message.ROWID desc nulls last
