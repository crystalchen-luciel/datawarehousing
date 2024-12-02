select
    unique_key,
    format_datetime(
        '%Y-%m-%d', parse_datetime('%m/%d/%Y %I:%M:%S %p', created_date)
    ) as created_date,
    upper(borough) as borough,
    upper(city) as city,
    upper(complaint_type) as complaint_type,
    incident_zip
from {{ ref("nyc_311_data") }}
where
    unique_key is not null
    and borough is not null
    and city is not null
    and incident_zip is not null
