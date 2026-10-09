-- Sessionisation: a session is a run of listens with no gap longer than session_gap_minutes
-- between consecutive listen starts. Also derives skip-like short plays, where derivable:
--   * basis 'played_ms': the client reported the real playback time (Spotify extended history
--     imports); short when it is below short_play_fraction of the track duration;
--   * basis 'next_listen': otherwise, the next listen in the same session began before
--     short_play_fraction of this track's duration had elapsed. Gaps shorter than
--     min_listen_gap_seconds (30) are timestamp collisions (importer batches, offline syncs
--     submitting many listens with the same or bunched timestamps), not plays: no client may
--     submit a listen played for under 30 seconds, so such listens are not derivable.
-- ListenBrainz clients only submit a listen after half the track (or 4 minutes) has played, so
-- most skips never become listens at all; these flags catch the subset the data reveals.
{% set gap_seconds = (var('session_gap_minutes') | int) * 60 %}

with base as (
    select *
    from {{ ref('int_listens__enriched') }}
    where not is_duplicate_scrobble
),

neighbours as (
    select
        *,
        lag(listened_at) over (partition by user_key order by listened_at, event_id) as prev_at,
        lead(listened_at) over (partition by user_key order by listened_at, event_id) as next_at
    from base
),

flagged as (
    select
        *,
        case
            when prev_at is null then 1
            when {{ seconds_between('prev_at', 'listened_at') }} > {{ gap_seconds }} then 1
            else 0
        end as is_session_start,
        case
            when next_at is null then null
            else {{ seconds_between('listened_at', 'next_at') }}
        end as seconds_to_next_listen
    from neighbours
),

numbered as (
    select
        *,
        sum(is_session_start) over (
            partition by user_key
            order by listened_at, event_id
            rows between unbounded preceding and current row
        ) as session_number
    from flagged
)

select
    event_id,
    user_key,
    listened_at,
    listen_date,
    week_start,
    artist_key,
    primary_artist_mbid,
    artist_name,
    track_name,
    recording_mbid,
    release_mbid,
    release_group_mbid,
    track_duration_ms,
    duration_source,
    submission_client,
    music_service,
    is_mbid_mapped,
    user_key || '-' || cast(session_number as varchar) as session_id,
    session_number,
    is_session_start = 1 as is_session_start,
    seconds_to_next_listen,
    played_ms,
    coalesce(seconds_to_next_listen <= {{ gap_seconds }}, false) as has_next_in_session,
    short_play_basis,
    short_play_basis is not null as is_short_play_derivable,
    case
        when short_play_basis = 'played_ms'
            then played_ms < {{ var('short_play_fraction') }} * track_duration_ms
        when short_play_basis = 'next_listen'
            then seconds_to_next_listen < {{ var('short_play_fraction') }} * track_duration_ms / 1000.0
        else false
    end as is_short_play
from (
    select
        *,
        case
            when played_ms is not null and track_duration_ms is not null then 'played_ms'
            when seconds_to_next_listen <= {{ gap_seconds }}
                and seconds_to_next_listen >= {{ var('min_listen_gap_seconds') }}
                and track_duration_ms is not null
                then 'next_listen'
        end as short_play_basis
    from numbered
) as based
