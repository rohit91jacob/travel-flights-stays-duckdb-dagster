{#- Current version of every listing, from the type-2 snapshot. -#}
select
    listing_id,
    city,
    hotel_name,
    star_rating,
    rating,
    rating_description,
    review_count,
    locality,
    nearest_landmark,
    distance_to_landmark_km,
    nightly_price_inr,
    taxes_and_fees_inr,
    total_nightly_price_inr,
    has_tax_breakdown,
    captured_on,
    dataset_version,
    'INR' as currency_code,
    cast(dbt_valid_from as timestamp) as valid_from
from {{ ref('snap_hotel_listings') }}
where dbt_valid_to is null
