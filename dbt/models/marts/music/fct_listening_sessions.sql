-- One row per listening session (30-minute inactivity gap by default).
-- ended_at = start of the last listen plus its duration when known.
with listens as (
    select * from {{ ref('int_listens__sessionised') }}
),

last_listen as (
    select
        session_id,
        listened_at,
        track_duration_ms,
        row_number() over (partition by session_id order by listened_at desc, event_id desc) as rn
    from listens
),

agg as (
    select
        session_id,
        min(user_key) as user_key,
        min(listened_at) as started_at,
        max(listened_at) as last_listen_at,
        count(*) as listens,
        count(distinct artist_key) as distinct_artists,
        sum(case when is_short_play_derivable then 1 else 0 end) as short_play_derivable_listens,
        sum(case when is_short_play then 1 else 0 end) as short_plays
    from listens
    group by session_id
)

select
    cast(a.session_id as varchar) as session_id,
    cast(a.user_key as varchar) as user_key,
    a.started_at,
    case
        when l.track_duration_ms is not null
            then {{ dbt.dateadd('second', 'cast(l.track_duration_ms / 1000 as bigint)', 'a.last_listen_at') }}
        else a.last_listen_at
    end as ended_at,
    cast(a.started_at as date) as session_date,
    cast(a.listens as bigint) as listens,
    cast(a.distinct_artists as bigint) as distinct_artists,
    cast(
        {{ seconds_between('a.started_at', 'a.last_listen_at') }}
        + coalesce(l.track_duration_ms / 1000, 0) as double
    ) / 60.0 as duration_minutes,
    cast(a.short_play_derivable_listens as bigint) as short_play_derivable_listens,
    cast(a.short_plays as bigint) as short_plays
from agg as a
inner join last_listen as l on a.session_id = l.session_id and l.rn = 1
