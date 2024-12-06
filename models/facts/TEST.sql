--select * 
--from {{ ref("dim_date") }}
--where date_created = '2011-12-19'

--select * 
--from {{ ref("cleaned_311_data") }}
--where created_date = '2011-12-19'
    --retuirned 2 rows

--select * 
--from {{ ref("cleaned_housing_maintenance_data") }}
--where received_date = '2011-12-19'
    --- returned 3426 rows

select * 
from {{ ref("cleaned_311_data") }}
where unique_key = 19245645