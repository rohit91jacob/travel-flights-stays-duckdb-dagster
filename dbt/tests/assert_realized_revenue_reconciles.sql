-- Room revenue must be identical whether summed per booking or per occupied night.
with by_booking as (
    select sum(realized_room_revenue_eur) as revenue
    from {{ ref('fct_hotel_bookings') }}
),

by_night as (
    select sum(room_revenue_eur) as revenue
    from {{ ref('int_stays__occupied_room_nights') }}
)

select
    by_booking.revenue as booking_revenue,
    by_night.revenue as night_revenue
from by_booking
cross join by_night
where abs(coalesce(by_booking.revenue, 0) - coalesce(by_night.revenue, 0)) > 0.01
