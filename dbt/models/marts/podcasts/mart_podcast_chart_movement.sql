-- Rank movement per show, country and daily snapshot versus the previous snapshot of that
-- country's chart. rank_change > 0 means the show climbed. The first snapshot of a country is
-- the 'baseline' (nothing to compare with).
select
    snapshot_date,
    cast(snapshot_seq as bigint) as snapshot_seq,
    cast(country as varchar) as country,
    cast(chart_rank as bigint) as chart_rank,
    cast(podcast_id as varchar) as podcast_id,
    cast(podcast_name as varchar) as podcast_name,
    cast(chart_genre as varchar) as chart_genre,
    cast(previous_rank as bigint) as previous_rank,
    cast(previous_rank - chart_rank as bigint) as rank_change,
    cast(
        case
            when is_first_snapshot then 'baseline'
            when previous_rank is null and is_first_appearance then 'new'
            when previous_rank is null then 're-entry'
            when previous_rank > chart_rank then 'up'
            when previous_rank < chart_rank then 'down'
            else 'same'
        end as varchar
    ) as movement
from {{ ref('int_chart__entries_sequenced') }}
