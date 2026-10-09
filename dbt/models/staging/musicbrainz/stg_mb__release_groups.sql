select
    release_group_mbid,
    title,
    primary_type,
    first_release_date as first_release_date_raw,
    {{ iso_date('first_release_date') }} as first_release_date,
    artist_credit
from {{ source('raw', 'mb_release_groups') }}
