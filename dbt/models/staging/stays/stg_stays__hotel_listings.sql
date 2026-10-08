with source as (
    select * from {{ source('raw', 'hotel_listings') }}
),

parsed as (
    select
        city,
        rating,
        rating_description,
        star_rating,
        source_file,
        source_row_number,
        dataset_version,
        _loaded_at,
        trim(hotel_name) as hotel_name,
        reviews as review_count,
        trim(regexp_replace(location, '^In\s+', '')) as locality,
        trim(nearest_landmark) as nearest_landmark,
        trim(distance_to_landmark_raw) as distance_text,
        try_cast(replace(price_raw, ',', '') as integer) as nightly_price_inr,
        try_cast(replace(tax_raw, ',', '') as integer) as taxes_and_fees_inr,
        cast('{{ var("hotel_listings_captured_on") }}' as date) as captured_on
    from source
)

select
    city,
    hotel_name,
    rating,
    rating_description,
    review_count,
    star_rating,
    locality,
    nearest_landmark,
    distance_text,
    nightly_price_inr,
    taxes_and_fees_inr,
    captured_on,
    source_file,
    source_row_number,
    dataset_version,
    _loaded_at,
    md5(lower(city) || '|' || lower(hotel_name)) as listing_id,
    case
        when distance_text like '% km'
            then cast(regexp_extract(distance_text, '^([0-9.]+)', 1) as decimal(8, 2))
        when distance_text like '% m'
            then cast(cast(regexp_extract(distance_text, '^([0-9.]+)', 1) as double) / 1000 as decimal(8, 2))
    end as distance_to_landmark_km,
    nightly_price_inr + coalesce(taxes_and_fees_inr, 0) as total_nightly_price_inr,
    taxes_and_fees_inr is not null as has_tax_breakdown
from parsed
