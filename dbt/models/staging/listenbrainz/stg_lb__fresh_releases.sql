-- Releases ListenBrainz lists as released in the last N days, latest snapshot per release.
with ranked as (
    select
        *,
        row_number() over (partition by release_mbid order by snapshot_date desc) as rn
    from {{ source('raw', 'lb_fresh_releases') }}
)

select
    snapshot_date,
    release_mbid,
    release_group_mbid,
    release_name,
    artist_credit_name,
    primary_artist_mbid,
    release_date,
    primary_type,
    sitewide_listen_count
from ranked
where rn = 1
