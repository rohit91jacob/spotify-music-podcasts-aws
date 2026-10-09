-- Reconciliation: episodes we parsed from each show's RSS feed versus the episode count and
-- latest-episode time Apple publishes through the iTunes Lookup API.
select
    cast(c.podcast_id as varchar) as podcast_id,
    cast(c.podcast_name as varchar) as podcast_name,
    cast(d.published_episode_count as bigint) as itunes_episode_count,
    cast(c.episodes_in_feed as bigint) as rss_episode_count,
    cast(c.episodes_in_feed - d.published_episode_count as bigint) as episode_count_difference,
    d.latest_release_at as itunes_latest_release_at,
    c.latest_episode_at as rss_latest_episode_at,
    cast(
        {{ seconds_between('c.latest_episode_at', 'd.latest_release_at') }} / 3600.0 as double
    ) as latest_episode_hours_difference
from {{ ref('mart_episode_cadence') }} as c
inner join {{ ref('dim_podcast') }} as d on c.podcast_id = d.podcast_id
where d.published_episode_count is not null
