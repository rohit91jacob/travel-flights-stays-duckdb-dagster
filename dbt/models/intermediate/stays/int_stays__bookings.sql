{#-
    One row per booking, reconstructed from its events as of the latest loaded event date.
    Bookings whose final status has not been replayed yet are 'Booked' with an unknown
    outcome, so metrics never use information from the future of the replay clock.
-#}
with events as (
    select * from {{ ref('stg_stays__booking_events') }}
),

created as (
    select * from events
    where event_type = 'booking_created'
),

finalised as (
    select
        booking_id,
        event_date as status_changed_on,
        event_status as final_status
    from events
    where event_type = 'status_changed'
),

as_of as (
    select max(event_date) as as_of_date from events
)

select
    created.booking_id,
    created.hotel_code,
    created.hotel_name,
    created.booking_date,
    created.arrival_date,
    created.checkout_date,
    created.lead_time_days,
    created.nights,
    created.weekend_nights,
    created.week_nights,
    created.adults,
    created.children,
    created.babies,
    created.guests,
    created.meal_plan,
    created.country_code,
    created.market_segment,
    created.distribution_channel,
    created.is_repeated_guest,
    created.previous_cancellations,
    created.previous_bookings_not_canceled,
    created.reserved_room_type,
    created.assigned_room_type,
    created.is_room_reassigned,
    created.booking_changes,
    created.deposit_type,
    created.agent_id,
    created.company_id,
    created.days_in_waiting_list,
    created.customer_type,
    created.adr_eur,
    created.required_car_parking_spaces,
    created.total_of_special_requests,
    created.source_is_canceled,
    created.has_no_guests,
    created.is_zero_night_stay,
    created.has_negative_adr,
    finalised.final_status,
    finalised.status_changed_on,
    as_of.as_of_date,
    coalesce(finalised.final_status, 'Booked') as current_status,
    finalised.final_status is not null as is_outcome_known,
    case
        when finalised.final_status is null then null
        else finalised.final_status in ('Canceled', 'No-Show')
    end as is_canceled
from created
left join finalised on created.booking_id = finalised.booking_id
cross join as_of
