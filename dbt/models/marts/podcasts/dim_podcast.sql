-- Shows that appeared on any tracked chart, with iTunes Lookup metadata where available.
with charted as (
    select
        podcast_id,
        min(podcast_name) as chart_name,
        min(publisher) as chart_publisher,
        min(chart_genre) as chart_genre,
        min(snapshot_date) as first_charted_on,
        max(snapshot_date) as last_charted_on,
        count(distinct country) as countries_charted
    from {{ ref('stg_apple__chart_entries') }}
    group by podcast_id
)

select
    cast(c.podcast_id as varchar) as podcast_id,
    cast(coalesce(s.podcast_name, c.chart_name) as varchar) as podcast_name,
    cast(coalesce(s.publisher, c.chart_publisher) as varchar) as publisher,
    cast(coalesce(s.primary_genre, c.chart_genre) as varchar) as primary_genre,
    cast(s.published_episode_count as bigint) as published_episode_count,
    s.latest_release_at,
    c.first_charted_on,
    c.last_charted_on,
    cast(c.countries_charted as bigint) as countries_charted
from charted as c
left join {{ ref('stg_apple__shows') }} as s on c.podcast_id = s.podcast_id
