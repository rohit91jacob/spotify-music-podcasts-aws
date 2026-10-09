-- Episode release cadence per show from its RSS feed (latest fetch). The median gap uses the
-- most recent full episodes only; trailers and bonus items are excluded.
{% set n = var('cadence_recent_episodes') %}
with episodes as (
    select
        podcast_id,
        fetched_date,
        published_at,
        duration_seconds,
        episode_type
    from {{ ref('stg_rss__episodes') }}
    where published_at is not null
),

full_eps as (
    select
        *,
        lag(published_at) over (partition by podcast_id order by published_at) as prev_published_at,
        row_number() over (partition by podcast_id order by published_at desc) as full_recency
    from episodes
    where episode_type = 'full'
),

gaps as (
    select
        podcast_id,
        {{ percentile(seconds_between('prev_published_at', 'published_at') ~ ' / 86400.0', 0.5) }} as median_gap_days
    from full_eps
    where full_recency <= {{ n }} and prev_published_at is not null
    group by podcast_id
),

agg as (
    select
        podcast_id,
        max(fetched_date) as fetched_date,
        count(*) as episodes_in_feed,
        sum(case when episode_type = 'full' then 1 else 0 end) as full_episodes_in_feed,
        min(published_at) as oldest_episode_at,
        max(published_at) as latest_episode_at,
        sum(
            case
                when {{ days_between('cast(published_at as date)', 'fetched_date') }} between 0 and 29 then 1
                else 0
            end
        ) as episodes_last_30_days,
        sum(
            case
                when {{ days_between('cast(published_at as date)', 'fetched_date') }} between 0 and 89 then 1
                else 0
            end
        ) as episodes_last_90_days,
        avg(cast(duration_seconds as double)) / 60.0 as avg_duration_minutes
    from episodes
    group by podcast_id
)

select
    cast(a.podcast_id as varchar) as podcast_id,
    cast(d.podcast_name as varchar) as podcast_name,
    cast(d.primary_genre as varchar) as primary_genre,
    a.fetched_date,
    cast(a.episodes_in_feed as bigint) as episodes_in_feed,
    cast(a.full_episodes_in_feed as bigint) as full_episodes_in_feed,
    a.oldest_episode_at,
    a.latest_episode_at,
    cast({{ days_between('cast(a.latest_episode_at as date)', 'a.fetched_date') }} as bigint) as days_since_latest_episode,
    cast(a.episodes_last_30_days as bigint) as episodes_last_30_days,
    cast(a.episodes_last_90_days as bigint) as episodes_last_90_days,
    cast(g.median_gap_days as double) as median_gap_days,
    cast(a.avg_duration_minutes as double) as avg_duration_minutes,
    cast(
        case
            when g.median_gap_days is null then 'insufficient history'
            when g.median_gap_days <= 1.5 then 'daily'
            when g.median_gap_days <= 4.5 then 'several per week'
            when g.median_gap_days <= 10 then 'weekly'
            when g.median_gap_days <= 20 then 'fortnightly'
            when g.median_gap_days <= 45 then 'monthly'
            else 'irregular'
        end as varchar
    ) as cadence
from agg as a
left join gaps as g on a.podcast_id = g.podcast_id
left join {{ ref('dim_podcast') }} as d on a.podcast_id = d.podcast_id
