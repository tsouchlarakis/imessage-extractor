drop table if exists message_user;
create table message_user as

with m as (
    select m.ROWID as message_id
           , datetime(`date` / 1000000000 + 978307200, 'unixepoch', 'localtime') as ts
           , trim(replace(replace(replace(m.text, '￼', ''), char(13), ' '), char(10), ' ')) as text  -- char(13) = carriage return, char(10) = line break
           , m.associated_message_type
           , m.balloon_bundle_id
           , m.service
           , case when m.is_from_me = 1 then true when m.is_from_me = 0 then false else null end as is_from_me
           , coalesce(thread_origins.is_thread_origin, false) as is_thread_origin
           , case when m.thread_originator_guid is not null then true else false end as is_threaded_reply
           , thread_origins.thread_original_message_id
           , case when m.cache_has_attachments = 1 then true
                  when m.cache_has_attachments = 0 then false
                  else null
             end as has_attachment
           , m.attributedBody as attributed_body
           , m.was_data_detected
           , m.cache_has_attachments
    from message m
    left join (
        -- Get the ROWID for all messages that have a thread_originator_guid
        select ROWID as threaded_reply_message_id, true as is_threaded_reply
        from message
        where thread_originator_guid is not null
    ) as threaded_replies
      on m.ROWID = threaded_replies.threaded_reply_message_id
    left join (
        -- Get a boolean flag for all messages that are the origin of a thread. These messages
        -- will not have a thread_originator_guid, because at the time that they are sent,
        -- they are not yet part of a thread
        select distinct m1.ROWID as thread_original_message_id, true as is_thread_origin
        from message m1
        join message m2
          on m1.guid = m2.thread_originator_guid
    ) as thread_origins
      on m.ROWID = thread_origins.thread_original_message_id
),

m_join_chat_contacts as (
    select m.message_id
           , c.chat_identifier
           , n.contact_name
           , m.ts
           , date(m.ts) as dt
           , text
           , m.service
           , m.is_from_me
           , case when m.associated_message_type in (2000, 2001, 2002, 2003, 2004, 2005, 3000, 3001, 3002, 3003, 3004, 3005) then true
                  else false
             end as is_emote
           , case when m.balloon_bundle_id like '%PeerPaymentMessagesExtension' then 'apple_cash'
                  when m.associated_message_type = 2 and m.balloon_bundle_id like '%imessagepoll%' then 'poll'
                  when m.associated_message_type in (2, 3) and m.balloon_bundle_id like '%gamepigeon%' then 'game_pigeon'
                  when m.associated_message_type in (2, 3) and m.balloon_bundle_id like '%messages.business.extension%' then 'business_extension'
                  when m.associated_message_type = 3 and m.text like '%earned an achievement.' then 'activity'
                  when m.associated_message_type = 3 and m.text like '%completed a workout.' then 'activity'
                  when m.associated_message_type = 3 and m.text like '%closed all three Activity rings.' then 'activity'
                  when m.associated_message_type = 3 and m.text like 'Requested % with Apple Pay.' then 'apple_cash'
                  when m.associated_message_type = 3 and (m.text like '%poll%' or m.text like '%voted%') then 'poll'
                  when m.associated_message_type = 3 and m.text = 'Cup Pong' then 'game_pigeon'
                  when m.associated_message_type = 3 and m.text = '8 Ball' then 'game_pigeon'
                  when m.associated_message_type = 3 and m.text = '(null)' then 'null_message'
                  when m.associated_message_type = 1000 and m.cache_has_attachments = 1 and m.was_data_detected = 1 then 'sticker'
                  when m.associated_message_type = 2000 then 'emote_love'
                  when m.associated_message_type = 2001 then 'emote_like'
                  when m.associated_message_type = 2002 then 'emote_dislike'
                  when m.associated_message_type = 2003 then 'emote_laugh'
                  when m.associated_message_type = 2004 then 'emote_emphasis'
                  when m.associated_message_type = 2005 then 'emote_question'
                  when m.associated_message_type = 3000 then 'emote_remove_heart'
                  when m.associated_message_type = 3001 then 'emote_remove_like'
                  when m.associated_message_type = 3002 then 'emote_remove_dislike'
                  when m.associated_message_type = 3003 then 'emote_remove_laugh'
                  when m.associated_message_type = 3004 then 'emote_remove_emphasis'
                  when m.associated_message_type = 3005 then 'emote_remove_question'
                  else null
             end as message_special_type
           , associated_message_type
           , case when c.chat_identifier like 'chat%' then true
                  else false
             end as is_group_chat
           , case when text like 'http:%'
                    or text like 'https:%'
                  then true
                  else false
             end as is_url
           , case when length(text) = 0 then true
                  when m.text is null then true
                  else false
             end as has_no_text
           , is_thread_origin
           , is_threaded_reply
           , thread_original_message_id
           , has_attachment
    from chat c
    join (
        select chat_id, message_id
      from (
        -- Use a window function to avoid one-to-many message_id: chat_id mappings
        select chat_id
               , message_id
               , row_number() over(partition by message_id order by message_date desc) as r
        from chat_message_join
      ) cm_join
      where r = 1
    ) cm_mapping on c.ROWID = cm_mapping.chat_id
    join m
      on cm_mapping.message_id = m.message_id
    left join contacts_user n
      on c.chat_identifier = n.chat_identifier
),

m2 as (
    select message_id
          , chat_identifier
          , contact_name
          , ts
          , dt
          , case when text = '' then null
                  when text = '�' then null
                  when is_emote = 1 then null
                  when is_url = 1 then null
                  when has_no_text = 1 then null
                  else text
            end as text
          , service
          , is_from_me
          , is_group_chat
          , case when is_emote = 0 and is_url = 0 and message_special_type is null and has_no_text = 0
                      then true
                  else false
            end as is_text
          , has_no_text
          , is_emote
          , message_special_type
          , associated_message_type
          , is_url
          , is_thread_origin
          , is_threaded_reply
          , thread_original_message_id
          , case when has_attachment = 1 and is_emote = 0 then 1 else 0 end as has_attachment
    from m_join_chat_contacts
)

select message_id
       , chat_identifier
       , contact_name
       , ts
       , dt
       , text
       , service
       , is_from_me
       , is_group_chat
       , is_text
       , has_no_text
       , is_emote
       , message_special_type
       , associated_message_type
       , is_url
       , is_thread_origin
       , is_threaded_reply
       , thread_original_message_id
       , has_attachment
       , case when has_attachment = 1 and is_url = 0 and is_text = 0 then 1 else 0 end as is_attachment
       , case when has_attachment = 1 and is_url = 0 and message_special_type is null then 1 else 0 end as has_attachment_image
       , case when has_attachment = 1 and is_url = 0 and message_special_type is null and is_text = 0 then 1 else 0 end as is_attachment_image
from m2
order by message_id desc nulls last
