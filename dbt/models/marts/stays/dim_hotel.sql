select
    hotel_code,
    hotel_name,
    hotel_type,
    region_name,
    country_code,
    currency_code
from {{ ref('ref_hotels') }}
