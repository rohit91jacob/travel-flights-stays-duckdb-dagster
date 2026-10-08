{#- Demand, cancellations and price by hotel and arrival month. -#}
with bookings as (
    select
        *,
        cast(date_trunc('month', arrival_date) as date) as arrival_month
    from {{ ref('fct_hotel_bookings') }}
)

select
    hotel_code,
    arrival_month,
    count(*) as bookings,
    count(*) filter (where is_canceled) as cancellations,
    count(*) filter (where current_status = 'Check-Out') as completed_stays,
    cast(year(arrival_month) as integer) as arrival_year,
    cast(month(arrival_month) as integer) as arrival_month_number,
    cast(
        count(*) filter (where is_canceled) * 1.0
        / nullif(count(*) filter (where is_outcome_known), 0) as decimal(6, 4)
    ) as cancellation_rate,
    cast(sum(nights) filter (where current_status = 'Check-Out') as bigint) as room_nights,
    cast(sum(realized_room_revenue_eur) as decimal(14, 2)) as realized_room_revenue_eur,
    cast(
        sum(realized_room_revenue_eur)
        / nullif(sum(nights) filter (where current_status = 'Check-Out'), 0) as decimal(10, 2)
    ) as realized_adr_eur,
    cast(avg(lead_time_days) as decimal(8, 1)) as avg_lead_time_days
from bookings
group by hotel_code, arrival_month
