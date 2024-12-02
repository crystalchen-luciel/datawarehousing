SELECT
            Row_number() OVER() AS dim_space_id, *
FROM
           (SELECT distinct space_type
                        FROM {{ref('cleaned_housing_maintenance_data')}})