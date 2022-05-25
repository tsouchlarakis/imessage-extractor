drop view if exists daily_summary_from_who_vw;
create view daily_summary_from_who_vw as

select dt
       , is_from_me
       , sum(messages) as messages
       , sum(text_messages) as text_messages
       , sum(group_chat_messages) as group_chat_messages
       , sum(group_chat_text_messages) as group_chat_text_messages
       , sum(imessages) as imessages
       , sum(sms) as sms
       , sum(emotes) as emotes
       , sum(emotes_love) as emotes_love
       , sum(emotes_likes) as emotes_likes
       , sum(emotes_dislikes) as emotes_dislikes
       , sum(emotes_laugh) as emotes_laugh
       , sum(emotes_emphasis) as emotes_emphasis
       , sum(emotes_question) as emotes_question
       , sum(emotes_remove_love) as emotes_remove_love
       , sum(emotes_remove_like) as emotes_remove_like
       , sum(emotes_remove_dislike) as emotes_remove_dislike
       , sum(emotes_remove_laugh) as emotes_remove_laugh
       , sum(emotes_remove_emphasis) as emotes_remove_emphasis
       , sum(emotes_remove_question) as emotes_remove_question
       , sum(urls) as urls
       , sum(app_for_imessage) as app_for_imessage
       , sum(thread_origins) as thread_origins
       , sum(threaded_replies) as threaded_replies
       , sum(messages_containing_attachment) as messages_containing_attachment
       , sum(messages_attachment_only) as messages_attachment_only
       , sum(messages_containing_attachment_image) as messages_containing_attachment_image
       , sum(messages_image_attachment_only) as messages_image_attachment_only
       , sum(tokens) as tokens
       , sum(characters) as characters
from daily_summary_contact_from_who_vw
group by dt, is_from_me
