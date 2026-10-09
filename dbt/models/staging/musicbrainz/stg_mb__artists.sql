select
    artist_mbid,
    name as artist_name,
    sort_name,
    artist_type,
    country,
    begin_date,
    disambiguation,
    fetched_on
from {{ source('raw', 'mb_artists') }}
