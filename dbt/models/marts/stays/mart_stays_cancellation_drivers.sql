{#-
    Cancellation rate by candidate driver, per hotel and for both hotels together ('ALL').
    Only bookings with a known outcome count. `lift_vs_hotel_rate` > 1 means the segment
    cancels more often than the hotel overall. Descriptive, not causal.
-#}
{%- set drivers = [
    ("deposit_type", "deposit_type"),
    ("lead_time_band", "case when lead_time_days < 7 then '0-6 days' when lead_time_days < 30 then '7-29 days' when lead_time_days < 90 then '30-89 days' when lead_time_days < 180 then '90-179 days' when lead_time_days < 365 then '180-364 days' else '365+ days' end"),
    ("market_segment", "market_segment"),
    ("distribution_channel", "distribution_channel"),
    ("customer_type", "customer_type"),
    ("guest_history", "case when is_repeated_guest then 'repeat guest' else 'first-time guest' end"),
    ("previous_cancellations", "case when previous_cancellations = 0 then 'none' else '1 or more' end"),
    ("special_requests", "case when total_of_special_requests = 0 then 'none' else '1 or more' end"),
    ("booking_changes", "case when booking_changes = 0 then 'none' else '1 or more' end"),
    ("parking_requested", "case when required_car_parking_spaces > 0 then 'yes' else 'no' end"),
    ("room_reassigned", "case when is_room_reassigned then 'yes' else 'no' end"),
] -%}

with known as (
    select * from {{ ref('fct_hotel_bookings') }}
    where is_outcome_known
),

scoped as (
    select
        hotel_code,
        is_canceled,
        deposit_type,
        lead_time_days,
        market_segment,
        distribution_channel,
        customer_type,
        is_repeated_guest,
        previous_cancellations,
        total_of_special_requests,
        booking_changes,
        required_car_parking_spaces,
        is_room_reassigned
    from known
    union all
    select
        'ALL' as hotel_code,
        is_canceled,
        deposit_type,
        lead_time_days,
        market_segment,
        distribution_channel,
        customer_type,
        is_repeated_guest,
        previous_cancellations,
        total_of_special_requests,
        booking_changes,
        required_car_parking_spaces,
        is_room_reassigned
    from known
),

hotel_rates as (
    select
        hotel_code,
        count(*) as hotel_bookings,
        avg(case when is_canceled then 1.0 else 0.0 end) as hotel_cancellation_rate
    from scoped
    group by hotel_code
),

by_driver as (
    {%- for name, expression in drivers %}
        select
            hotel_code,
            '{{ name }}' as driver,
            cast({{ expression }} as varchar) as driver_value,
            count(*) as bookings,
            count(*) filter (where is_canceled) as cancellations
        from scoped
        group by hotel_code, driver_value
        {% if not loop.last %}union all{% endif %}
    {%- endfor %}
)

select
    by_driver.hotel_code,
    by_driver.driver,
    by_driver.driver_value,
    by_driver.bookings,
    by_driver.cancellations,
    cast(by_driver.cancellations * 1.0 / by_driver.bookings as decimal(6, 4)) as cancellation_rate,
    cast(by_driver.bookings * 1.0 / hotel_rates.hotel_bookings as decimal(6, 4)) as share_of_bookings,
    cast(
        (by_driver.cancellations * 1.0 / by_driver.bookings)
        / nullif(hotel_rates.hotel_cancellation_rate, 0) as decimal(8, 3)
    ) as lift_vs_hotel_rate
from by_driver
inner join hotel_rates on by_driver.hotel_code = hotel_rates.hotel_code
