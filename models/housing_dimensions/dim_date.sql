select row_number() over () as dim_date_id, *
from
    (
        select distinct
            received_date as date_created,
            extract(month from date(received_date)) as month,
            extract(year from date(received_date)) as year,
            extract(quarter from date(received_date)) as quarter,
            extract(day from date(received_date)) as day
        from {{ ref("cleaned_housing_maintenance_data") }}
    )