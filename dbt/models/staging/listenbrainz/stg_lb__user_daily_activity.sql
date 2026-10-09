-- Published per-user daily listen counts (ListenBrainz listening-activity stats), latest snapshot.
with ranked as (
    select
        *,
        row_number() over (
            partition by user_key, activity_date order by snapshot_date desc
        ) as rn
    from {{ source('raw', 'lb_user_daily_activity') }}
)

select
    snapshot_date,
    user_key,
    activity_date,
    published_listen_count,
    stats_last_updated
from ranked
where rn = 1
