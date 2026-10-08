with source as (
    select * from {{ source('raw', 'flight_fare_quotes') }}
),

normalised as (
    select
        departure_date,
        cabin_class,
        source_file,
        source_row_number,
        dataset_version,
        _loaded_at,
        trim(airline) as airline_name,
        upper(trim(carrier_code)) as carrier_code,
        trim(flight_number) as flight_number,
        trim(source_city) as origin_city,
        trim(destination_city) as destination_city,
        trim(time_taken_raw) as time_taken_raw,
        trim(regexp_replace(stop_raw, '\s+', ' ', 'g')) as stop_text,
        try_strptime(trim(dep_time_raw), '%H:%M')::time as departure_time,
        try_strptime(trim(arr_time_raw), '%H:%M')::time as arrival_time,
        try_cast(replace(price_raw, ',', '') as integer) as fare_inr
    from source
),

parsed as (
    select
        *,
        md5(source_file || ':' || source_row_number::varchar) as fare_quote_id,
        cast('{{ var("flights_observed_on") }}' as date) as observed_on,
        carrier_code || '-' || flight_number as flight_code,
        case
            when regexp_full_match(time_taken_raw, '\d+h \d+m')
                then
                    (regexp_extract(time_taken_raw, '^(\d+)h', 1))::integer * 60
                    + (regexp_extract(time_taken_raw, '(\d+)m$', 1))::integer
        end as duration_minutes,
        case
            when lower(stop_text) like 'non-stop%' then 0
            when lower(stop_text) like '1-stop%' then 1
            when lower(stop_text) like '2+-stop%' then 2
        end as stops,
        nullif(trim(regexp_extract(stop_text, '(?i)via (.*)$', 1)), '') as via_point
    from normalised
)

select
    fare_quote_id,
    departure_date,
    observed_on,
    cabin_class,
    airline_name,
    carrier_code,
    flight_number,
    flight_code,
    origin_city,
    destination_city,
    departure_time,
    arrival_time,
    duration_minutes,
    stops,
    via_point,
    fare_inr,
    time_taken_raw,
    stop_text,
    source_file,
    source_row_number,
    dataset_version,
    _loaded_at,
    date_diff('day', observed_on, departure_date) as days_to_departure,
    case
        when stops = 0 then 'non-stop'
        when stops = 1 then '1 stop'
        when stops = 2 then '2+ stops'
    end as stops_label,
    case
        when hour(departure_time) < 4 then 'late_night'
        when hour(departure_time) < 8 then 'early_morning'
        when hour(departure_time) < 12 then 'morning'
        when hour(departure_time) < 16 then 'afternoon'
        when hour(departure_time) < 20 then 'evening'
        else 'night'
    end as departure_time_band,
    duration_minutes is null as is_duration_malformed,
    (departure_date + departure_time + to_minutes(duration_minutes))::time
    = arrival_time as is_arrival_time_consistent
from parsed
