-- Artists heard by the panel. MusicBrainz metadata where the listen was mapped to an MBID.
with listened as (
    select
        artist_key,
        max(primary_artist_mbid) as artist_mbid,
        count(*) as listens_total,
        count(distinct user_key) as listeners_total,
        min(listened_at) as first_listened_at,
        max(listened_at) as last_listened_at
    from {{ ref('int_listens__sessionised') }}
    group by artist_key
),

credited_names as (
    select
        artist_key,
        artist_name,
        row_number() over (partition by artist_key order by count(*) desc, artist_name) as rn
    from {{ ref('int_listens__sessionised') }}
    where artist_name is not null
    group by artist_key, artist_name
)

select
    cast(l.artist_key as varchar) as artist_key,
    cast(l.artist_mbid as varchar) as artist_mbid,
    cast(coalesce(a.artist_name, n.artist_name) as varchar) as artist_name,
    cast(a.artist_type as varchar) as artist_type,
    cast(a.country as varchar) as country,
    cast(coalesce(g.primary_genre, case when l.artist_mbid is null then 'unmapped' else 'unknown' end) as varchar) as primary_genre,
    cast(l.listens_total as bigint) as listens_total,
    cast(l.listeners_total as bigint) as listeners_total,
    l.first_listened_at,
    l.last_listened_at
from listened as l
left join credited_names as n on l.artist_key = n.artist_key and n.rn = 1
left join {{ ref('stg_mb__artists') }} as a on l.artist_mbid = a.artist_mbid
left join {{ ref('int_artist__primary_genre') }} as g on l.artist_mbid = g.artist_mbid
