select row_number() over () as dim_location_id, *
from
    (
        select distinct borough, post_code as zip_code, council_district, block, problem_id
                    FROM {{ref('cleaned_housing_maintenance_data')}})
    
