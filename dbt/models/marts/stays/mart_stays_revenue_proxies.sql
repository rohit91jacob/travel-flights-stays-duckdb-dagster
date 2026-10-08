{#-
    Monthly room revenue KPIs from completed stays.

    What the data supports: room-nights sold, room revenue and ADR.
    What it does not: occupancy and RevPAR need the hotels' room inventory, which the source
    does not publish. The *_proxy columns therefore assume capacity = the most rooms ever
    observed occupied on a single night at that hotel. That is a lower bound on capacity, so
    the proxies are upper bounds on true occupancy and RevPAR.
-#}
with nightly as (
    select
        hotel_code,
        stay_night,
        count(*) as occupied_rooms,
        sum(room_revenue_eur) as room_revenue
    from {{ ref('int_stays__occupied_room_nights') }}
    group by hotel_code, stay_night
),

peak as (
    select
        hotel_code,
        max(occupied_rooms) as peak_occupied_rooms
    from nightly
    group by hotel_code
),

coverage as (
    select
        hotel_code,
        date_trunc('month', min(arrival_date)) as first_month,
        date_trunc('month', max(arrival_date)) as last_month
    from {{ ref('fct_hotel_bookings') }}
    group by hotel_code
),

monthly as (
    select
        hotel_code,
        cast(date_trunc('month', stay_night) as date) as stay_month,
        cast(sum(occupied_rooms) as bigint) as room_nights_sold,
        sum(room_revenue) as room_revenue,
        max(occupied_rooms) as max_occupied_rooms
    from nightly
    group by hotel_code, stay_month
)

select
    monthly.hotel_code,
    monthly.stay_month,
    monthly.room_nights_sold,
    peak.peak_occupied_rooms,
    monthly.max_occupied_rooms,
    cast(day(last_day(monthly.stay_month)) as integer) as days_in_month,
    -- The first month misses stays that began before the data window; months after the last
    -- arrival month only hold the tail of earlier stays.
    (
        monthly.stay_month > coverage.first_month
        and monthly.stay_month <= coverage.last_month
    ) as is_complete_month,
    cast(monthly.room_revenue as decimal(14, 2)) as room_revenue_eur,
    cast(monthly.room_revenue / monthly.room_nights_sold as decimal(10, 2)) as adr_eur,
    cast(
        monthly.room_nights_sold * 1.0 / day(last_day(monthly.stay_month)) as decimal(10, 2)
    ) as avg_occupied_rooms,
    cast(
        monthly.room_nights_sold * 1.0
        / (day(last_day(monthly.stay_month)) * peak.peak_occupied_rooms) as decimal(6, 4)
    ) as occupancy_proxy,
    cast(
        monthly.room_revenue
        / (day(last_day(monthly.stay_month)) * peak.peak_occupied_rooms) as decimal(10, 2)
    ) as revpar_proxy_eur
from monthly
inner join peak on monthly.hotel_code = peak.hotel_code
inner join coverage on monthly.hotel_code = coverage.hotel_code
