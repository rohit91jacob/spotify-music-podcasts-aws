-- Chart entries with the position of their snapshot within the country's snapshot sequence and
-- the show's rank in the previous snapshot of the same country chart (null = not charting).
with snapshots as (
    select
        country,
        snapshot_date,
        dense_rank() over (partition by country order by snapshot_date) as snapshot_seq
    from (select distinct country, snapshot_date from {{ ref('stg_apple__chart_entries') }}) as s
),

entries as (
    select e.*, s.snapshot_seq
    from {{ ref('stg_apple__chart_entries') }} as e
    inner join snapshots as s
        on e.country = s.country and e.snapshot_date = s.snapshot_date
),

first_seen as (
    select country, podcast_id, min(snapshot_seq) as first_seq
    from entries
    group by country, podcast_id
)

select
    cur.snapshot_date,
    cur.snapshot_seq,
    cur.country,
    cur.chart_rank,
    cur.podcast_id,
    cur.podcast_name,
    cur.publisher,
    cur.chart_genre,
    prev.chart_rank as previous_rank,
    f.first_seq = cur.snapshot_seq as is_first_appearance,
    cur.snapshot_seq = 1 as is_first_snapshot
from entries as cur
left join entries as prev
    on
        cur.country = prev.country
        and cur.podcast_id = prev.podcast_id
        and prev.snapshot_seq = cur.snapshot_seq - 1
inner join first_seen as f
    on cur.country = f.country and cur.podcast_id = f.podcast_id
