-- Listens with a track duration (from the listen itself, else MusicBrainz) and a flag for
-- duplicate scrobbles: the same recording again from the same user within a few seconds.
with listens as (
    select * from {{ ref('stg_lb__listens') }}
),

recordings as (
    select recording_mbid, length_ms from {{ ref('stg_mb__recordings') }}
),

ordered as (
    select
        l.*,
        coalesce(l.duration_ms, r.length_ms) as track_duration_ms,
        case
            when l.duration_ms is not null then 'listen'
            when r.length_ms is not null then 'musicbrainz'
        end as duration_source,
        coalesce(l.recording_msid, l.recording_mbid, lower(l.track_name)) as track_identity,
        lag(l.listened_at) over (
            partition by l.user_key order by l.listened_at, l.event_id
        ) as prev_listened_at,
        lag(coalesce(l.recording_msid, l.recording_mbid, lower(l.track_name))) over (
            partition by l.user_key order by l.listened_at, l.event_id
        ) as prev_track_identity
    from listens as l
    left join recordings as r on l.recording_mbid = r.recording_mbid
)

select
    event_id,
    user_key,
    listened_at,
    listen_date,
    cast({{ dbt.date_trunc('week', 'listen_date') }} as date) as week_start,
    artist_key,
    primary_artist_mbid,
    artist_name,
    track_name,
    release_name,
    recording_mbid,
    release_mbid,
    release_group_mbid,
    track_duration_ms,
    duration_source,
    played_ms,
    submission_client,
    music_service,
    is_mbid_mapped,
    coalesce(
        prev_track_identity = track_identity
        and {{ seconds_between('prev_listened_at', 'listened_at') }}
        < {{ var('duplicate_scrobble_seconds') }},
        false
    ) as is_duplicate_scrobble
from ordered
