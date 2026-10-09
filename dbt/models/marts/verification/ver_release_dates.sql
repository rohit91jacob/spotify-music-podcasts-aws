-- Reconciliation: release dates ListenBrainz lists for fresh releases versus the
-- first-release-date MusicBrainz publishes for the same release group. The group's date is the
-- earliest of its releases, so it can be earlier than a particular edition's date.
select
    cast(f.release_mbid as varchar) as release_mbid,
    cast(f.release_group_mbid as varchar) as release_group_mbid,
    cast(f.release_name as varchar) as release_name,
    f.release_date as listenbrainz_release_date,
    rg.first_release_date as musicbrainz_first_release_date,
    cast(rg.first_release_date_raw as varchar) as musicbrainz_first_release_date_raw,
    cast({{ days_between('rg.first_release_date', 'f.release_date') }} as bigint) as days_difference
from {{ ref('stg_lb__fresh_releases') }} as f
inner join {{ ref('stg_mb__release_groups') }} as rg
    on f.release_group_mbid = rg.release_group_mbid
