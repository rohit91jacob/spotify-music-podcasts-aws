-- One row per listen. artist_key falls back to the credited name when ListenBrainz could not
-- map the listen to a MusicBrainz artist.
select
    event_id,
    user_key,
    listened_at,
    cast(listened_at as date) as listen_date,
    inserted_at,
    recording_msid,
    artist_name,
    track_name,
    release_name,
    recording_mbid,
    release_mbid,
    release_group_mbid,
    primary_artist_mbid,
    artist_mbids,
    coalesce(primary_artist_mbid, 'name:' || lower(trim(artist_name))) as artist_key,
    duration_ms,
    played_ms,
    submission_client,
    music_service,
    recording_mbid is not null as is_mbid_mapped
from {{ source('raw', 'listen_events') }}
where listened_at is not null
