{#-
    Type-2 history of each booking's status, derived deterministically from the event log.
    Building it from events (instead of a dbt snapshot of current state) keeps it exact under
    backfills and reruns: the history never depends on when or how often dbt ran.
-#}
with events as (
    select
        booking_id,
        hotel_code,
        event_status as status,
        event_date,
        event_sequence
    from {{ ref('stg_stays__booking_events') }}
)

select
    booking_id,
    hotel_code,
    status,
    event_date as valid_from,
    lead(event_date) over (partition by booking_id order by event_date, event_sequence) as valid_to,
    md5(booking_id || ':' || event_sequence::varchar) as booking_status_version_id,
    row_number() over (partition by booking_id order by event_date, event_sequence) as version_number,
    lead(event_date) over (partition by booking_id order by event_date, event_sequence) is null as is_current
from events
