-- iTunes Lookup metadata per show, latest lookup (any storefront; the US storefront wins ties).
with ranked as (
    select
        *,
        row_number() over (
            partition by podcast_id
            order by lookup_date desc, case when country = 'us' then 0 else 1 end, country
        ) as rn
    from {{ source('raw', 'podcast_shows') }}
)

select
    lookup_date,
    country as lookup_country,
    podcast_id,
    podcast_name,
    publisher,
    primary_genre,
    published_episode_count,
    latest_release_at,
    explicitness,
    has_feed_url
from ranked
where rn = 1
