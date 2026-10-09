-- Weekly share of panel listens by the artist's primary MusicBrainz genre.
-- 'unknown' = mapped artist without genre tags; 'unmapped' = listen not matched to an artist MBID.
with weekly as (
    select
        week_start,
        primary_genre as genre,
        count(*) as listens,
        count(distinct user_key) as listeners,
        count(distinct artist_key) as artists
    from {{ ref('fct_listens') }}
    group by week_start, primary_genre
),

totals as (
    select week_start, sum(listens) as week_listens
    from weekly
    group by week_start
)

select
    w.week_start,
    cast(w.genre as varchar) as genre,
    cast(w.listens as bigint) as listens,
    cast(w.listeners as bigint) as listeners,
    cast(w.artists as bigint) as artists,
    cast(t.week_listens as bigint) as week_listens,
    {{ ratio('w.listens', 't.week_listens') }} as listen_share,
    cast(row_number() over (partition by w.week_start order by w.listens desc, w.genre) as bigint) as genre_rank
from weekly as w
inner join totals as t on w.week_start = t.week_start
