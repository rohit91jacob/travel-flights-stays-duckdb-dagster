with days as (
    select
        cast(
            unnest(
                range(
                    cast('{{ var("dim_date_start") }}' as timestamp),
                    cast('{{ var("dim_date_end") }}' as timestamp) + interval 1 day,
                    interval 1 day
                )
            ) as date
        ) as date_day
)

select
    date_day,
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    cast(year(date_day) as integer) as year_number,
    cast(quarter(date_day) as integer) as quarter_number,
    cast(month(date_day) as integer) as month_number,
    strftime(date_day, '%B') as month_name,
    cast(date_trunc('month', date_day) as date) as month_start,
    cast(isoyear(date_day) as integer) as iso_year,
    cast(week(date_day) as integer) as iso_week,
    cast(isodow(date_day) as integer) as iso_day_of_week,
    strftime(date_day, '%A') as day_name,
    isodow(date_day) in (6, 7) as is_weekend
from days
