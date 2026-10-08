with source as (
    select * from {{ source('raw', 'hotel_booking_events') }}
),

renamed as (
    select
        booking_id,
        event_type,
        event_date,
        event_status,
        hotel as hotel_name,
        booking_date,
        arrival_date,
        lead_time as lead_time_days,
        stays_in_weekend_nights as weekend_nights,
        stays_in_week_nights as week_nights,
        adults,
        babies,
        country as country_code,
        market_segment,
        distribution_channel,
        previous_cancellations,
        previous_bookings_not_canceled,
        reserved_room_type,
        assigned_room_type,
        booking_changes,
        deposit_type,
        agent as agent_id,
        company as company_id,
        days_in_waiting_list,
        customer_type,
        adr as adr_eur,
        required_car_parking_spaces,
        total_of_special_requests,
        reservation_status,
        reservation_status_date,
        source_file,
        source_row_number,
        dataset_version,
        _loaded_at,
        coalesce(children, 0) as children,
        -- The source paper documents 'Undefined' and 'SC' as the same thing: no meal package.
        case when meal = 'Undefined' then 'SC' else meal end as meal_plan,
        is_repeated_guest = 1 as is_repeated_guest,
        is_canceled = 1 as source_is_canceled,
        children is null as is_children_missing,
        case hotel
            when 'Resort Hotel' then 'H1'
            when 'City Hotel' then 'H2'
        end as hotel_code
    from source
)

select
    *,
    md5(booking_id || ':' || event_type) as booking_event_id,
    case when event_type = 'booking_created' then 1 else 2 end as event_sequence,
    weekend_nights + week_nights as nights,
    arrival_date + (weekend_nights + week_nights) as checkout_date,
    adults + children + babies as guests,
    reserved_room_type <> assigned_room_type as is_room_reassigned,
    adults + children + babies = 0 as has_no_guests,
    weekend_nights + week_nights = 0 as is_zero_night_stay,
    adr_eur < 0 as has_negative_adr
from renamed
