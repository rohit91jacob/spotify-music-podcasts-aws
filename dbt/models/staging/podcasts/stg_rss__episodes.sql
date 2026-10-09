-- Episodes listed in each show's RSS feed, from the most recent fetch of that feed.
with latest as (
    select podcast_id, max(fetched_date) as fetched_date
    from {{ source('raw', 'podcast_episodes') }}
    group by podcast_id
)

select
    e.fetched_date,
    e.podcast_id,
    e.episode_key,
    e.published_at,
    e.duration_seconds,
    lower(coalesce(e.episode_type, 'full')) as episode_type,
    e.feed_status
from {{ source('raw', 'podcast_episodes') }} as e
inner join latest as l
    on e.podcast_id = l.podcast_id and e.fetched_date = l.fetched_date
