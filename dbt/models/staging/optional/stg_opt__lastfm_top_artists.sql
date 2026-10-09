-- Optional (LASTFM_API_KEY): empty when the key is not configured.
select snapshot_date, rank, artist_name, artist_mbid, playcount, listeners
from {{ source('raw', 'lastfm_top_artists') }}
