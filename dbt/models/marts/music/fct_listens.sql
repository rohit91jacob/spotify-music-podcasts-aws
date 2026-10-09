-- One row per (non-duplicate) listen, with session and short-play attributes.
select
    cast(s.event_id as varchar) as event_id,
    cast(s.user_key as varchar) as user_key,
    s.listened_at,
    s.listen_date,
    s.week_start,
    cast(s.artist_key as varchar) as artist_key,
    cast(d.artist_name as varchar) as artist_name,
    cast(d.primary_genre as varchar) as primary_genre,
    cast(s.track_name as varchar) as track_name,
    cast(s.recording_mbid as varchar) as recording_mbid,
    cast(s.release_group_mbid as varchar) as release_group_mbid,
    cast(s.track_duration_ms as bigint) as track_duration_ms,
    cast(s.duration_source as varchar) as duration_source,
    cast(s.session_id as varchar) as session_id,
    cast(s.seconds_to_next_listen as bigint) as seconds_to_next_listen,
    cast(s.played_ms as bigint) as played_ms,
    cast(s.short_play_basis as varchar) as short_play_basis,
    s.is_short_play_derivable,
    s.is_short_play,
    s.is_mbid_mapped,
    cast(s.submission_client as varchar) as submission_client,
    cast(s.music_service as varchar) as music_service
from {{ ref('int_listens__sessionised') }} as s
inner join {{ ref('dim_artist') }} as d on s.artist_key = d.artist_key
