SELECT
            Row_number() OVER() AS dim_status_id, *
FROM
           (SELECT distinct complaint_status
            FROM {{ref('cleaned_housing_maintenance_data')}})