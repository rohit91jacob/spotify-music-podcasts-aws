-- How many of the tracked country charts each show is on, using each country's latest snapshot.
with latest as (
    select country, max(snapshot_date) as snapshot_date
    from {{ ref('stg_apple__chart_entries') }}
    group by country
),

current_entries as (
    select e.*
    from {{ ref('stg_apple__chart_entries') }} as e
    inner join latest as l on e.country = l.country and e.snapshot_date = l.snapshot_date
)

select
    cast(podcast_id as varchar) as podcast_id,
    cast(min(podcast_name) as varchar) as podcast_name,
    cast(count(distinct country) as bigint) as countries,
    cast(min(chart_rank) as bigint) as best_rank,
    cast(avg(cast(chart_rank as double)) as double) as avg_rank,
    cast({{ dbt.listagg('country', "','", 'order by country') }} as varchar) as country_list
from current_entries
group by podcast_id
