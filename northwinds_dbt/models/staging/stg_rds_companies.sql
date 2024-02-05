WITH source AS (
    SELECT * FROM {{ source('rds', 'customers')}}
)

, renamed AS (
    SELECT
        concat('rds-', company_name) company_id
        , company_name
        , max(address) as address
        , max(city) as city
        , max(postal_code) as postal_code
        , max(country) as country

    FROM
        source
    group by
        company_name
)

SELECT * FROM renamed
