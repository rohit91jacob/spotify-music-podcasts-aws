-- Chart longevity per show and country across every daily snapshot collected so far.
-- current_streak = consecutive snapshots up to and including the latest one (0 if not charting).
with entries as (
    select * from {{ ref('int_chart__entries_sequenced') }}
),

latest as (
    select
        country,
        max(snapshot_seq) as latest_seq,
        count(distinct snapshot_date) as snapshots_in_country
    from entries
    group by country
),

islands as (
    select
        country,
        podcast_id,
        snapshot_seq,
        snapshot_seq - row_number() over (
            partition by country, podcast_id order by snapshot_seq
        ) as island
    from entries
),

streaks as (
    select country, podcast_id, island, max(snapshot_seq) as island_end, count(*) as island_len
    from islands
    group by country, podcast_id, island
),

agg as (
    select
        country,
        podcast_id,
        min(podcast_name) as podcast_name,
        min(snapshot_date) as first_seen_on,
        max(snapshot_date) as last_seen_on,
        count(*) as snapshots_on_chart,
        min(chart_rank) as best_rank,
        avg(cast(chart_rank as double)) as avg_rank
    from entries
    group by country, podcast_id
)

select
    cast(a.country as varchar) as country,
    cast(a.podcast_id as varchar) as podcast_id,
    cast(a.podcast_name as varchar) as podcast_name,
    a.first_seen_on,
    a.last_seen_on,
    cast(a.snapshots_on_chart as bigint) as snapshots_on_chart,
    cast(l.snapshots_in_country as bigint) as snapshots_in_country,
    {{ ratio('a.snapshots_on_chart', 'l.snapshots_in_country') }} as share_of_snapshots,
    cast(a.best_rank as bigint) as best_rank,
    cast(a.avg_rank as double) as avg_rank,
    cast(cur.chart_rank as bigint) as latest_rank,
    cast(coalesce(s.island_len, 0) as bigint) as current_streak,
    cur.podcast_id is not null as on_latest_chart
from agg as a
inner join latest as l on a.country = l.country
left join entries as cur
    on a.country = cur.country and a.podcast_id = cur.podcast_id and cur.snapshot_seq = l.latest_seq
left join streaks as s
    on a.country = s.country and a.podcast_id = s.podcast_id and s.island_end = l.latest_seq
