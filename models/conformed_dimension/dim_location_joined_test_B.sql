with
    location_311 as (select * from {{ ref('dim_location_311') }}),
    location_housing as (select * from {{ ref('dim_location') }}),
    dim_location_joined as (
        select
            row_number() over() as dim_location_joined_id, *
            from
            (select distinct
            coalesce(loc311.borough, loc_housing.borough) as borough,
            coalesce(loc311.zip_code, loc_housing.zip_code) as zip_code,
            city, loc_housing.council_district, loc_housing.block
        from location_311 loc311
        full outer join location_housing loc_housing
            on loc311.borough = loc_housing.borough
            and loc311.zip_code = loc_housing.zip_code 
            ))

    select  * from dim_location_joined