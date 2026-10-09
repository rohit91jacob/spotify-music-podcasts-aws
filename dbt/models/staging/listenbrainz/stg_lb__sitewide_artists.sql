select
    snapshot_date,
    stats_range,
    rank,
    artist_mbid,
    artist_name,
    listen_count,
    from_ts,
    to_ts,
    last_updated
from {{ source('raw', 'lb_sitewide_artists') }}
