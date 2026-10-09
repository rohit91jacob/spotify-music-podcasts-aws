-- Skip-like short plays per artist and week, where derivable (next listen in the same session
-- and a known track duration). Only artist-weeks with enough derivable listens are kept.
with agg as (
    select
        week_start,
        artist_key,
        sum(case when is_short_play_derivable then 1 else 0 end) as derivable_listens,
        sum(case when is_short_play then 1 else 0 end) as short_plays,
        count(*) as listens
    from {{ ref('fct_listens') }}
    group by week_start, artist_key
)

select
    a.week_start,
    cast(a.artist_key as varchar) as artist_key,
    cast(d.artist_name as varchar) as artist_name,
    cast(a.listens as bigint) as listens,
    cast(a.derivable_listens as bigint) as derivable_listens,
    cast(a.short_plays as bigint) as short_plays,
    {{ ratio('a.short_plays', 'a.derivable_listens') }} as short_play_rate
from agg as a
inner join {{ ref('dim_artist') }} as d on a.artist_key = d.artist_key
where a.derivable_listens >= {{ var('min_derivable_listens') }}
