-- New-release discovery lag: for releases ListenBrainz lists as freshly released, how long after
-- the release date the panel first played them, and how much in the first week.
-- Matched on release group (any edition of the release counts).
with fresh as (
    select
        release_group_mbid,
        min(release_date) as release_date,
        min(release_name) as release_name,
        min(artist_credit_name) as artist_credit_name,
        min(primary_type) as primary_type
    from {{ ref('stg_lb__fresh_releases') }}
    where release_group_mbid is not null
    group by release_group_mbid
),

plays as (
    select
        f.release_group_mbid,
        l.listened_at,
        l.user_key,
        {{ days_between('f.release_date', 'l.listen_date') }} as days_after_release
    from fresh as f
    inner join {{ ref('fct_listens') }} as l on f.release_group_mbid = l.release_group_mbid
)

select
    cast(f.release_group_mbid as varchar) as release_group_mbid,
    cast(f.release_name as varchar) as release_name,
    cast(f.artist_credit_name as varchar) as artist_credit_name,
    cast(f.primary_type as varchar) as primary_type,
    f.release_date,
    min(p.listened_at) as first_listened_at,
    cast(min(case when p.days_after_release >= 0 then p.days_after_release end) as bigint) as discovery_lag_days,
    cast(count(*) as bigint) as panel_listens,
    cast(count(distinct p.user_key) as bigint) as panel_listeners,
    cast(sum(case when p.days_after_release between 0 and 6 then 1 else 0 end) as bigint) as listens_first_7_days,
    cast(sum(case when p.days_after_release < 0 then 1 else 0 end) as bigint) as listens_before_release_date
from fresh as f
inner join plays as p on f.release_group_mbid = p.release_group_mbid
group by f.release_group_mbid, f.release_name, f.artist_credit_name, f.primary_type, f.release_date
