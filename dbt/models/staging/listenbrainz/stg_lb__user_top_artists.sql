with latest as (
    select user_key, max(snapshot_date) as snapshot_date
    from {{ source('raw', 'lb_user_top_artists') }}
    group by user_key
)

select t.*
from {{ source('raw', 'lb_user_top_artists') }} as t
inner join latest as l
    on t.user_key = l.user_key and t.snapshot_date = l.snapshot_date
