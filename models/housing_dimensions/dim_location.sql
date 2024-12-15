select row_number() over () as dim_location_id, *
from
    (
        select distinct housing.borough, post_code as zip_code, council_district, block, city
        from {{ ref("cleaned_housing_maintenance_data") }} as housing
        left join {{ref("cleaned_311_data")}} as nyc_311 on nyc_311.unique_key = housing.problem_id)
    
    
