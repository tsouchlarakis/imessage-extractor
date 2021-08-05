drop view if exists imessage.message_vw;

create or replace view imessage.message_vw as

select
    message_uid
    , "ROWID"
    , chat_identifier
    , contact_name
    , message_date
    , "text"
    , n_characters
    , case when is_emote = false and is_url = false and message_special_type is null then array_length(regexp_split_to_array("text", '\s+'), 1)
           when is_empty then 0
           else null
      end as n_words
    , service
    , source
    , is_from_me
    , is_group_chat
    , case when is_emote = false and is_url = false and is_empty = false then true else false end as is_text
    , is_empty
    , is_emote
    , message_special_type
    , is_url
from
(
    select
        m.message_uid
        , m."ROWID"
        , c.chat_identifier
        , n.contact_name
        , m.date as message_date
        , m."text"
        , length(m."text") as n_characters
        , m.service
        , m.source
        , m.is_from_me
        , case when m.associated_message_type in (2000, 2001, 2002, 2003, 2004, 2005, 3000, 3001, 3002, 3003, 3004, 3005) then true
               else false
          end as is_emote
        , case when m."text" ilike '$%completed a workout.' then 'workout_notification'
               when m."text" ilike '$%closed all three Activity rings.' then 'workout_notification'
               when m."text" ilike '$%earned an achievement.' then 'workout_notification'
               when m.associated_message_type = 3 then 'app_for_imessage'
               when m.associated_message_type = 2000 then 'love'
               when m.associated_message_type = 2001 then 'like'
               when m.associated_message_type = 2002 then 'dislike'
               when m.associated_message_type = 2003 then 'laugh'
               when m.associated_message_type = 2004 then 'emphasis'
               when m.associated_message_type = 2005 then 'question'
               when m.associated_message_type = 3000 then 'remove_heart'
               when m.associated_message_type = 3001 then 'remove_like'
               when m.associated_message_type = 3002 then 'remove_dislike'
               when m.associated_message_type = 3003 then 'remove_laugh'
               when m.associated_message_type = 3004 then 'remove_emphasis'
               when m.associated_message_type = 3005 then 'remove_question'
               else null
          end as message_special_type
        , case when c.chat_identifier ilike 'chat%' then true
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
    from
        {pg_schema}.chat c
    join
        {pg_schema}.chat_message_join cm_join
        on c."ROWID" = cm_join.chat_id
        and c.source = cm_join.source
    join (
            select
                message_uid
                , "ROWID"
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
                , associated_message_type
                , "service"
                , source
                , case when is_from_me = 1 then true when is_from_me = 0 then false else null end as is_from_me
            from
                {pg_schema}.message
        ) m
        on cm_join.message_id = m."ROWID"
        and cm_join.source = m.source
    left join
        imessage.contact_names_vw n
        on c.chat_identifier = n.chat_identifier
) t1
order by
    message_date desc nulls last
