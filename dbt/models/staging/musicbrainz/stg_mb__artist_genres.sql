-- Artist tags that are on MusicBrainz's official genre list, with their vote counts.
-- Tags/genres are MusicBrainz supplementary data (CC BY-NC-SA 3.0).
select
    artist_mbid,
    tag as genre,
    votes
from {{ source('raw', 'mb_artist_tags') }}
where is_genre and votes > 0
