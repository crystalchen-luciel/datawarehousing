SELECT
      row_number() OVER() AS dim_complaint_type_id, *
FROM
    (SELECT distinct major_category, minor_category
    FROM {{ref('cleaned_housing_maintenance_data')}})
