with listings as (
    select
        *,
        case
            when star_rating is null then 'unrated'
            when star_rating <= 3 then '3-star and below'
            when star_rating = 4 then '4-star'
            else '5-star'
        end as star_band
    from {{ ref('dim_hotel_listing') }}
),

banded as (
    select
        city,
        star_band,
        count(*) as listings,
        median(total_nightly_price_inr) as median_total,
        quantile_cont(total_nightly_price_inr, 0.25) as p25_total,
        quantile_cont(total_nightly_price_inr, 0.75) as p75_total,
        avg(rating) as avg_rating,
        median(review_count) as median_reviews,
        min(captured_on) as captured_on
    from listings
    group by city, star_band

    union all

    select
        city,
        'all' as star_band,
        count(*) as listings,
        median(total_nightly_price_inr) as median_total,
        quantile_cont(total_nightly_price_inr, 0.25) as p25_total,
        quantile_cont(total_nightly_price_inr, 0.75) as p75_total,
        avg(rating) as avg_rating,
        median(review_count) as median_reviews,
        min(captured_on) as captured_on
    from listings
    group by city
)

select
    city,
    star_band,
    listings,
    captured_on,
    cast(median_total as decimal(12, 2)) as median_total_nightly_price_inr,
    cast(p25_total as decimal(12, 2)) as p25_total_nightly_price_inr,
    cast(p75_total as decimal(12, 2)) as p75_total_nightly_price_inr,
    cast(avg_rating as decimal(4, 2)) as avg_rating,
    cast(median_reviews as decimal(10, 1)) as median_review_count
from banded
