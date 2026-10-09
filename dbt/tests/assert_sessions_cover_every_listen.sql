-- Every non-duplicate listen belongs to exactly one session, and the session totals add up.
with by_session as (
    select session_id, count(*) as listens from {{ ref('fct_listens') }} group by session_id
)

select s.session_id, s.listens as session_listens, b.listens as fact_listens
from {{ ref('fct_listening_sessions') }} as s
full outer join by_session as b on s.session_id = b.session_id
where s.session_id is null or b.session_id is null or s.listens <> b.listens
