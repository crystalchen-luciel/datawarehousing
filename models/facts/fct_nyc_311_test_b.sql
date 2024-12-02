with
    location_joined as (select * from {{ ref("dim_location_joined_test_B") }}),
    date_311 as (select * from {{ ref("dim_date") }}),
    complaint_type_311 as (select * from {{ ref("dim_complaint_type_311") }} ),
    fct_nyc_311 as (select * from {{ ref("cleaned_311_data")}})

select distinct
    fct.unique_key,
    dim_location_joined_id,
    dim_date_id,
    dim_complaint_type_311_id
from fct_nyc_311 as fct
left join location_joined on fct.unique_key = location_joined.problem_id
left join date_311 on fct.created_date = date_311.date_created
left join complaint_type_311 on UPPER(fct.complaint_type) = UPPER(complaint_type_311.complaint_type)
where dim_location_joined_id is not null
order by unique_key asc

/* with
    location_311 as (select * from {{ ref("dim_location_311") }}),
    date_311 as (select * from {{ ref("dim_date") }}),
    complaint_type_311 as (select * from {{ ref("dim_complaint_type_311") }} ),
    fct_nyc_311 as (select *, created_date as date_created_311 from {{ ref("cleaned_311_data")}})

select distinct
    unique_key,
    dim_location_311_id,
    dim_date_id,
    dim_complaint_type_311_id
from fct_nyc_311 as fct
left join location_311 on fct.borough = location_311.borough
and fct.incident_zip = location_311.zip_code
and fct.city = location_311.city
left join date_311 on fct.date_created_311 = date_311.date_created
left join complaint_type_311 on UPPER(fct.complaint_type) = UPPER(complaint_type_311.complaint_type)
where unique_key is not null */

/* with 
    location_311 as (select * from {{ ref('dim_location_311') }}),
    location_housing as (select * from {{ ref('dim_location') }}),
    dim_location_joined as (
        select
            dim_location_id,
            coalesce(loc311.borough, loc_housing.borough) as borough,
            coalesce(loc311.zip_code, loc_housing.zip_code) as zip_code,
            loc311.city,
            loc_housing.council_district,
            loc_housing.block
        from location_311 loc311
        full outer join location_housing loc_housing
            on loc311.borough = loc_housing.borough
            and loc311.zip_code = loc_housing.zip_code

    ),

    dim_date as ( select * from {{ ref("dim_date") }}),
    dim_complaint_type_311 as (select * from {{ ref("dim_complaint_type_311") }}),
    fct_nyc_311 as (select *, created_date as date_created_311 from {{ ref("cleaned_311_data")}})

select 
    fct.unique_key, 
    dim_location_joined.dim_location_id, 
    dim_date.dim_date_id, 
    dim_complaint_type_311.dim_complaint_type_311_id
from fct_nyc_311 as fct
left join dim_location_joined
    on fct.borough = dim_location_joined.borough
    and fct.incident_zip = dim_location_joined.zip_code
left join dim_date 
    on fct.date_created_311 = dim_date.date_created
left join dim_complaint_type_311 
    on UPPER(fct.complaint_type) = UPPER(dim_complaint_type_311.complaint_type) */