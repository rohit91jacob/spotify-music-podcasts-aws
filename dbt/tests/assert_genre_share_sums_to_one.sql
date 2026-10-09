-- Genre shares are a partition of each week's listens.
select week_start, sum(listen_share) as total_share
from {{ ref('mart_genre_share') }}
group by week_start
having abs(sum(listen_share) - 1.0) > 1e-9
