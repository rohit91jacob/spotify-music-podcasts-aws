-- One row per (daily snapshot, country storefront, chart position) of Apple Podcasts' Top Shows.
select
    snapshot_date,
    country,
    chart_rank,
    podcast_id,
    podcast_name,
    publisher,
    chart_genre,
    chart_updated_at
from {{ source('raw', 'podcast_chart_entries') }}
