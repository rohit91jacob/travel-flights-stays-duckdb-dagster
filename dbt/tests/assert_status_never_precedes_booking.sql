-- A booking cannot be cancelled or checked out before it was made.
select
    booking_id,
    booking_date,
    status_changed_on
from {{ ref('fct_hotel_bookings') }}
where status_changed_on < booking_date
