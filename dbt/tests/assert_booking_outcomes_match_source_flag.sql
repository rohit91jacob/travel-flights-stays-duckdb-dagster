-- A replayed outcome must agree with the source's own is_canceled flag.
select
    booking_id,
    current_status,
    source_is_canceled
from {{ ref('int_stays__bookings') }}
where is_outcome_known and is_canceled <> source_is_canceled
