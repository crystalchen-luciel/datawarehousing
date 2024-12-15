
select row_number() over () as dim_location_311_id, *
from
    (
        select distinct nyc_311.borough, nyc_311.city, incident_zip as zip_code, council_district, block
        from {{ ref("cleaned_311_data") }} as nyc_311
        left join {{ref("cleaned_housing_maintenance_data")}} as housing on nyc_311.unique_key = housing.problem_id)
    
