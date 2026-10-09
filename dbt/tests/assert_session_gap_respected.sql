-- Consecutive listens of a user share a session exactly when they are at most the session gap
-- apart (sessionisation rule, 30 minutes by default).
{% set gap_seconds = (var('session_gap_minutes') | int) * 60 %}
with ordered as (
    select
        event_id,
        session_id,
        seconds_to_next_listen,
        lead(session_id) over (partition by user_key order by listened_at, event_id) as next_session_id
    from {{ ref('fct_listens') }}
)

select event_id, session_id, next_session_id, seconds_to_next_listen
from ordered
where next_session_id is not null
  and ((seconds_to_next_listen <= {{ gap_seconds }}) <> (session_id = next_session_id))
