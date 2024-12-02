SELECT
        row_number() OVER() AS dim_complaint_type_311_id, *
FROM
           (SELECT distinct complaint_type
            FROM {{ ref("cleaned_311_data")}}
            )
            