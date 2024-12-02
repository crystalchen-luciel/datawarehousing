with deduplicated_data as (
    select
    CAST(problem_id as int64) as problem_id,
    upper(major_category) as major_category,
    upper(minor_category) as minor_category,
    case
        when received_date IS NOT NULL 
        then format_date('%Y-%m-%d', parse_date('%m/%d/%Y', received_date))
        else NULL
    end as received_date,
    upper(space_type) as space_type,
    upper(complaint_status) as complaint_status,
    upper(borough) as borough,
    post_code,
    CAST(council_district as int64) as council_district,
    CAST(block as int64) as block,
    row_number() over (
        partition by problem_id
    ) as row_num
    from {{ ref('housing_maintenace_data') }}
    where
        problem_id is not null
        and post_code is not null
        and council_district is not null
        and received_date IS NOT NULL
)
select *
from deduplicated_data
where row_num = 1