-- Optional (SPOTIFY_CLIENT_ID/SECRET): catalogue metadata only; empty when not configured.
select snapshot_date, query_name, spotify_id, name
from {{ source('raw', 'spotify_artists') }}
