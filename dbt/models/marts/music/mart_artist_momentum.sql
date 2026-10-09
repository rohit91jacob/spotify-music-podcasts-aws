-- Week-over-week listens per artist. Weeks run Monday-Sunday (UTC). is_complete_week: the whole
-- week lies inside the data window. is_comparable_week: this week and the previous one are both
-- complete, so wow_change is meaningful (partial edge weeks would fake growth or decline).
with bounds as (
    select min(listen_date) as data_start, max(listen_date) as data_end
    from {{ ref('fct_listens') }}
),

weekly as (
    select
        week_start,
        artist_key,
        count(*) as listens,
        count(distinct user_key) as listeners
    from {{ ref('fct_listens') }}
    group by week_start, artist_key
),

with_prev as (
    select
        cur.week_start,
        cur.artist_key,
        cur.listens,
        cur.listeners,
        coalesce(prev.listens, 0) as prev_week_listens,
        coalesce(prev.listeners, 0) as prev_week_listeners
    from weekly as cur
    left join weekly as prev
        on
            cur.artist_key = prev.artist_key
            and prev.week_start = cast({{ dbt.dateadd('day', -7, 'cur.week_start') }} as date)
)

select
    w.week_start,
    cast(w.artist_key as varchar) as artist_key,
    cast(d.artist_name as varchar) as artist_name,
    cast(d.primary_genre as varchar) as primary_genre,
    cast(w.listens as bigint) as listens,
    cast(w.listeners as bigint) as listeners,
    cast(w.prev_week_listens as bigint) as prev_week_listens,
    cast(w.listens - w.prev_week_listens as bigint) as wow_change,
    {{ ratio('w.listens - w.prev_week_listens', 'w.prev_week_listens') }} as wow_pct,
    cast(
        row_number() over (
            partition by w.week_start order by w.listens - w.prev_week_listens desc, w.listens desc, w.artist_key
        ) as bigint
    ) as momentum_rank,
    (
        w.week_start >= b.data_start
        and cast({{ dbt.dateadd('day', 6, 'w.week_start') }} as date) < b.data_end
    ) as is_complete_week,
    (
        cast({{ dbt.dateadd('day', -7, 'w.week_start') }} as date) >= b.data_start
        and cast({{ dbt.dateadd('day', 6, 'w.week_start') }} as date) < b.data_end
    ) as is_comparable_week
from with_prev as w
inner join {{ ref('dim_artist') }} as d on w.artist_key = d.artist_key
cross join bounds as b
