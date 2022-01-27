drop view if exists message_user_text_vw;
create view message_user_text_vw as

select message_id
       , chat_identifier
       , contact_name
       , ts
       , dt
       , "text"
       , length(case when is_emote = false and is_url = false and is_empty = false and message_special_type is null
                          then "text"
                     else null
                end) as n_characters
       , case when is_emote = false and is_url = false and message_special_type is null
                   then (length(text) - length(replace(text, ' ', ''))) / length(' ') + 1
              when is_empty
                   then 0
              else null
         end as n_tokens
       , service
       , is_from_me
       , is_group_chat
       , is_threaded_reply
       , thread_original_message_id
from message_user
where is_text = true
  and is_empty = false
