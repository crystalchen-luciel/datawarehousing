SELECT
            Row_number() OVER() AS dim_location_311_id, *
FROM
           (SELECT distinct borough, city, incident_zip AS zip_code
            FROM {{ ref('cleaned_311_data')}})
