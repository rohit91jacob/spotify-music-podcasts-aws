-- An artist's primary genre: its most-voted MusicBrainz genre tag (ties: alphabetical).
with ranked as (
    select
        artist_mbid,
        genre,
        votes,
        row_number() over (partition by artist_mbid order by votes desc, genre) as rn
    from {{ ref('stg_mb__artist_genres') }}
)

select artist_mbid, genre as primary_genre, votes as primary_genre_votes
from ranked
where rn = 1
