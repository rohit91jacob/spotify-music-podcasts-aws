-- Each storefront snapshot is a ranking 1..n without gaps (n <= the requested chart size).
select snapshot_date, country, count(*) as entries, max(chart_rank) as max_rank
from {{ ref('mart_podcast_chart_movement') }}
group by snapshot_date, country
having max(chart_rank) <> count(*) or count(*) > 200
