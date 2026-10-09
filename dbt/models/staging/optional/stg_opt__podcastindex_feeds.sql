-- Optional (PODCASTINDEX_API_KEY/SECRET): empty when the keys are not configured.
select snapshot_date, podcast_id, podcastindex_id, episode_count, newest_item_at, language
from {{ source('raw', 'podcastindex_feeds') }}
