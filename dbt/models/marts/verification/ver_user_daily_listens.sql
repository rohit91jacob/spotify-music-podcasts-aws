-- Reconciliation: listens per user and UTC day counted from our Firehose -> Iceberg table versus
-- the counts ListenBrainz publishes (listening-activity stats). Only days fully covered by both
-- sides are comparable: after the first day we hold listens for, and before the day the
-- published statistics were last computed. ListenBrainz computes the statistics from a listens
-- snapshot taken a few days before `last_updated`, so listens inserted after that snapshot are
-- missing from the last days of the published range; is_settled excludes those days
-- (stats_settle_days, default 3).
with ours as (
    select user_key, listen_date, count(*) as our_listen_count
    from {{ ref('stg_lb__listens') }}
    group by user_key, listen_date
),

range_end as (
    select user_key, snapshot_date, max(activity_date) as range_end
    from {{ ref('stg_lb__user_daily_activity') }}
    group by user_key, snapshot_date
),

coverage as (
    select user_key, min(listen_date) as first_day
    from {{ ref('stg_lb__listens') }}
    group by user_key
)

select
    cast(p.user_key as varchar) as user_key,
    p.activity_date,
    cast(p.published_listen_count as bigint) as published_listen_count,
    cast(coalesce(o.our_listen_count, 0) as bigint) as our_listen_count,
    cast(coalesce(o.our_listen_count, 0) - p.published_listen_count as bigint) as difference,
    coalesce(
        p.activity_date > c.first_day
        and p.activity_date < cast(p.stats_last_updated as date),
        false
    ) as is_comparable,
    coalesce(
        p.activity_date > c.first_day
        and p.activity_date < cast(p.stats_last_updated as date)
        and p.activity_date <= cast({{ dbt.dateadd('day', -(var('stats_settle_days') | int), 'r.range_end') }} as date),
        false
    ) as is_settled
from {{ ref('stg_lb__user_daily_activity') }} as p
inner join range_end as r on p.user_key = r.user_key and p.snapshot_date = r.snapshot_date
left join ours as o on p.user_key = o.user_key and p.activity_date = o.listen_date
left join coverage as c on p.user_key = c.user_key
