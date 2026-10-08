select
    currency_code,
    currency_name,
    cast(minor_unit as integer) as minor_unit
from {{ ref('ref_currencies') }}
