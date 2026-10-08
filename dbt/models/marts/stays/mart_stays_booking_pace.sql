{#-
    On-the-books curve: for each arrival month, how many bookings existed N days before
    arrival (gross), how many of those had already been cancelled by then, and the net.
    `share_of_final_arrivals` compares the net position with the stays that checked out.
-#}
with bookings as (
    select * from {{ ref('fct_hotel_bookings') }}
),

thresholds as (
    select unnest([365, 270, 180, 120, 90, 60, 30, 14, 7, 0]) as days_before_arrival
),

on_the_books as (
    select
        bookings.hotel_code,
        thresholds.days_before_arrival,
        cast(date_trunc('month', bookings.arrival_date) as date) as arrival_month,
        count(*) filter (
            where bookings.lead_time_days >= thresholds.days_before_arrival
        ) as gross_bookings,
        count(*) filter (
            where
            bookings.lead_time_days >= thresholds.days_before_arrival
            and bookings.is_canceled
            and bookings.status_changed_on <= bookings.arrival_date - thresholds.days_before_arrival
        ) as cancelled_by_then
    from bookings
    cross join thresholds
    group by bookings.hotel_code, arrival_month, thresholds.days_before_arrival
),

finals as (
    select
        hotel_code,
        cast(date_trunc('month', arrival_date) as date) as arrival_month,
        count(*) filter (where current_status = 'Check-Out') as final_arrivals,
        bool_and(is_outcome_known) as all_outcomes_known
    from bookings
    group by hotel_code, arrival_month
)

select
    on_the_books.hotel_code,
    on_the_books.arrival_month,
    finals.final_arrivals,
    finals.all_outcomes_known,
    cast(on_the_books.days_before_arrival as integer) as days_before_arrival,
    on_the_books.gross_bookings,
    on_the_books.cancelled_by_then,
    on_the_books.gross_bookings - on_the_books.cancelled_by_then as net_bookings,
    cast(
        (on_the_books.gross_bookings - on_the_books.cancelled_by_then) * 1.0
        / nullif(finals.final_arrivals, 0) as decimal(8, 4)
    ) as share_of_final_arrivals
from on_the_books
inner join finals
    on
        on_the_books.hotel_code = finals.hotel_code
        and on_the_books.arrival_month = finals.arrival_month
