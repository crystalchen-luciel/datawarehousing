with deduplicated_data as (
    select
    CAST(unique_key as int64) as unique_key,
    format_datetime('%Y-%m-%d', PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', created_date)) as created_date,
    upper(borough) as borough,
    upper(city) as city,
    incident_zip,
    upper(complaint_type) as complaint_type,
    row_number() over (
        partition by unique_key
    ) as row_num
    from {{ ref('nyc_311_data') }}
    where
        unique_key is not null
        and borough is not null
        and city is not null
        and incident_zip is not null

)
select *
from deduplicated_data
--where row_num = 1
