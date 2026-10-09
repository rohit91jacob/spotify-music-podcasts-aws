-- Weekly share of listens by the age of the release group at listen time (MusicBrainz
-- first-release-date). 'unknown' covers unmapped listens, unlooked-up release groups and
-- partial dates (year or year-month only).
with aged as (
    select
        l.week_start,
        l.user_key,
        case
            when rg.first_release_date is null then 'unknown'
            when {{ days_between('rg.first_release_date', 'l.listen_date') }} < 0 then 'pre-release'
            when {{ days_between('rg.first_release_date', 'l.listen_date') }} <= 28 then '0-28 days'
            when {{ days_between('rg.first_release_date', 'l.listen_date') }} <= 365 then '29-365 days'
            when {{ days_between('rg.first_release_date', 'l.listen_date') }} <= 5 * 365 then '1-5 years'
            else 'over 5 years'
        end as release_age_bucket
    from {{ ref('fct_listens') }} as l
    left join {{ ref('stg_mb__release_groups') }} as rg
        on l.release_group_mbid = rg.release_group_mbid
),

weekly as (
    select week_start, release_age_bucket, count(*) as listens, count(distinct user_key) as listeners
    from aged
    group by week_start, release_age_bucket
)

select
    w.week_start,
    cast(w.release_age_bucket as varchar) as release_age_bucket,
    cast(w.listens as bigint) as listens,
    cast(w.listeners as bigint) as listeners,
    {{ ratio('w.listens', 'sum(w.listens) over (partition by w.week_start)') }} as listen_share
from weekly as w
