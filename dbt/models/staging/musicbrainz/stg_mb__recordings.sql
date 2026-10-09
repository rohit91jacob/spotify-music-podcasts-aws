select
    recording_mbid,
    title,
    length_ms,
    {{ iso_date('first_release_date') }} as first_release_date,
    is_video
from {{ source('raw', 'mb_recordings') }}
