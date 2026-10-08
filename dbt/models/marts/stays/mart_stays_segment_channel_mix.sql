with bookings as (
    select
        *,
        cast(year(arrival_date) as integer) as arrival_year
    from {{ ref('fct_hotel_bookings') }}
),

mix as (
    select
        hotel_code,
        arrival_year,
        market_segment,
        distribution_channel,
        count(*) as bookings,
        count(*) filter (where is_outcome_known) as bookings_with_known_outcome,
        count(*) filter (where is_canceled) as cancellations,
        avg(lead_time_days) as avg_lead_time,
        sum(realized_room_revenue_eur) as realized_revenue,
        sum(nights) filter (where current_status = 'Check-Out') as realized_room_nights
    from bookings
    group by hotel_code, arrival_year, market_segment, distribution_channel
)

select
    hotel_code,
    arrival_year,
    market_segment,
    distribution_channel,
    bookings,
    cancellations,
    cast(
        bookings * 1.0 / sum(bookings) over (partition by hotel_code, arrival_year) as decimal(6, 4)
    ) as share_of_hotel_year_bookings,
    cast(cancellations * 1.0 / nullif(bookings_with_known_outcome, 0) as decimal(6, 4)) as cancellation_rate,
    cast(avg_lead_time as decimal(8, 1)) as avg_lead_time_days,
    cast(realized_revenue as decimal(14, 2)) as realized_room_revenue_eur,
    cast(realized_revenue / nullif(realized_room_nights, 0) as decimal(10, 2)) as realized_adr_eur
from mix
