select row_number() over () as dim_location_311_id, *
from
    (
        select distinct borough, city, incident_zip as zip_code
        from {{ ref("cleaned_311_data") }}
    )
