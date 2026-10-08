{#- ADR and length of stay of completed stays, by hotel, reserved room type and LOS band. -#}
with stays as (
    select
        *,
        case
            when nights = 1 then '1 night'
            when nights <= 3 then '2-3 nights'
            when nights <= 7 then '4-7 nights'
            when nights <= 14 then '8-14 nights'
            else '15+ nights'
        end as los_band,
        case
            when nights = 1 then 1
            when nights <= 3 then 2
            when nights <= 7 then 3
            when nights <= 14 then 4
            else 5
        end as los_band_order
    from {{ ref('fct_hotel_bookings') }}
    where current_status = 'Check-Out' and nights > 0
)

select
    hotel_code,
    reserved_room_type,
    los_band,
    count(*) as stays,
    cast(sum(nights) as bigint) as room_nights,
    cast(los_band_order as integer) as los_band_order,
    cast(sum(adr_eur * nights) / sum(nights) as decimal(10, 2)) as adr_eur,
    cast(median(adr_eur) as decimal(10, 2)) as median_booking_adr_eur,
    cast(avg(nights) as decimal(8, 2)) as avg_length_of_stay,
    cast(median(nights) as decimal(8, 1)) as median_length_of_stay
from stays
group by hotel_code, reserved_room_type, los_band, los_band_order
