-- Daily session statistics across the panel.
select
    session_date,
    cast(count(*) as bigint) as sessions,
    cast(count(distinct user_key) as bigint) as active_users,
    cast(sum(listens) as bigint) as listens,
    cast(avg(cast(listens as double)) as double) as avg_listens_per_session,
    cast({{ percentile('duration_minutes', 0.5) }} as double) as median_session_minutes,
    cast({{ percentile('duration_minutes', 0.9) }} as double) as p90_session_minutes,
    cast(sum(short_plays) as bigint) as short_plays,
    cast(sum(short_play_derivable_listens) as bigint) as short_play_derivable_listens
from {{ ref('fct_listening_sessions') }}
group by session_date
