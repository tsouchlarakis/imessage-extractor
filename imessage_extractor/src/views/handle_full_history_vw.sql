drop view if exists imessage_current.handle_full_history_vw;

create or replace view imessage_current.handle_full_history_vw as

select
    map.handle_uid,
    h."ROWID",
    'imessage_current'::text as source,
    h.id,
    h.country,
    h.service,
    h.uncanonicalized_id,
    h.person_centric_id
from
    imessage_current.handle h
    left join
        imessage_current.map_handle_id map
        on h."ROWID" = map."ROWID"
        and map.source::text = 'imessage_current'::text

union

select
    map.handle_uid,
    h."ROWID",
    'imessage_20191126'::text as source,
    h.id,
    h.country,
    h.service,
    h.uncanonicalized_id,
    h.person_centric_id
from
    imessage_current.handle h
    left join
        imessage_current.map_handle_id map
        on h."ROWID" = map."ROWID"
        and map.source::text = 'imessage_20191126'::text

union

select
    map.handle_uid,
    h."ROWID",
    'imessage_20171001'::text as source,
    h.id,
    h.country,
    h.service,
    h.uncanonicalized_id,
    h.person_centric_id
from
    imessage_current.handle h
    left join
        imessage_current.map_handle_id map
        on h."ROWID" = map."ROWID"
        and map.source::text = 'imessage_20171001'::text

order by
    3 desc
