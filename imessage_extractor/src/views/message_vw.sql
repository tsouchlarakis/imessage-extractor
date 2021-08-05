drop view if exists imessage_test.message_vw;
create view imessage_test.message_vw as

select message_id
       , chat_identifier
       , contact_name
       , message_date
       , "text"
       , n_characters
       , case when is_emote = false and is_url = false and message_special_type is null then array_length(regexp_split_to_array("text", '\s+'), 1)
              when is_empty then 0
              else null
         end as n_tokens
       , service
       , is_from_me
       , is_group_chat
       , case when is_emote = false and is_url = false and is_empty = false then true else false end as is_text
       , is_empty
       , is_emote
       , message_special_type
       , is_url
       , is_thread
       , thread_original_message_id
       , has_attachment
from (
    select m.message_id
           , c._identifier as chat_identifier
           , n.contact_name
           , m.date as message_date
           , m."text"
           , length(m."text") as n_characters
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
    from imessage_test.chat c
    join imessage_test.chat_message_join cm_join
      on c."ROWID" = cm_join.chat_id
    join (
        select "ROWID" as message_id
                , "date"
                , btrim(
                    regexp_replace(
                        replace(
                            regexp_replace(
                                "text",
                                '\.{2,}',
                                '…',
                                'g'
                            ),
                            '￼',
                            ''
                        ),
                        '[\n\r]+',
                        ' ',
                        'g'
                    )
                ) as "text"
                , associated__type
                , "service"
                , case when is_from_me = 1 then true when is_from_me = 0 then false else null end as is_from_me
                , case when msg.thread_originator_guid is not null then true else false end as is_thread
                , thread_origins.thread_original_message_id
                , case when cache_has_attachments = 1 then true when cache_has_attachments = 0 then false else null end as has_attachment
        from imessage_test.message msg
        left join (
            select distinct t1.thread_originator_guid, t2."ROWID" as thread_original_message_id
            from imessage_test.message t1
            join imessage_test.message t2
                on t1.thread_originator_guid = t2.guid
        ) as thread_origins
        on msg.thread_originator_guid = thread_origins.thread_originator_guid
    ) m
    on cm_join.message_id = m.message_id
    left join imessage_test.contacts_vw n
           on c._identifier = n.chat_identifier
) t1
order by message_date desc nulls last
