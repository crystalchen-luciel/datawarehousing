with
    location as (select * from {{ ref("dim_location_joined") }}),
    complaint_type as (select * from {{ ref("dim_complaint_type") }}),
    date_ as (select * from {{ ref("dim_date") }}),
    space_ as (select * from {{ ref("dim_space") }}),
    status as (select * from {{ ref("dim_status") }}),
    fct_complaint as (select * from {{ ref("cleaned_housing_maintenance_data") }})
select distinct
    fct.problem_id,
    dim_location_joined_id,
    dim_date_id,
    dim_status_id,
    dim_space_id,
    dim_complaint_type_id,
    dim_location_joined_id
from fct_complaint as fct
left join date_ on fct.received_date = date_.date_created
left join status on fct.complaint_status = status.complaint_status
left join space_ on fct.space_type = space_.space_type
left join
    complaint_type
    on fct.major_category = complaint_type.major_category
    and fct.minor_category = complaint_type.minor_category
left join
    location
    on fct.post_code = location.zip_code
    and fct.council_district = location.council_district
    and fct.block = location.block

