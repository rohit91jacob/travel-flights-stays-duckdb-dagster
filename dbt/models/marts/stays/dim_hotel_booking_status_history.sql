select
    booking_status_version_id,
    booking_id,
    hotel_code,
    status,
    valid_from,
    valid_to,
    is_current,
    cast(version_number as integer) as version_number
from {{ ref('int_stays__booking_status_history') }}
