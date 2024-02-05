-- models/example_model.sql
WITH customers AS (
    SELECT * FROM {{ source('rds', 'customers')}}
    {% set name_parts = "contact_name" %}
),

companies AS (
    SELECT * FROM {{ ref('stg_rds_companies') }}
),

renamed AS (
    SELECT
        concat('rds-', customer_id) as customer_id
        {% for i in range(1, 3) %}
            {%- if i == 1 -%}
                , split_part(customers.contact_name, ' ', {{ i }}) as first_name 
            {% else %}
                , split_part(customers.contact_name, ' ', {{ i }}) as last_name
            
            {%- endif -%}
            
        {% endfor %}
        , replace(translate(phone, '(,),-,.', ''), ' ', '') as updated_phone
        , companies.company_id
    FROM
        customers
    INNER JOIN
        companies ON customers.company_name = companies.company_name
), 

clean_phone as (

    select
        customer_id
        , first_name
        , last_name
        , {{ clean_phone_number('updated_phone') }} as cleaned_phone
        , company_id
    from 
        renamed
),

format_phone as (
    select
        customer_id
        , first_name
        , last_name
        , {{ format_phone_number('cleaned_phone') }} as formatted_phone
        , company_id
    from
        clean_phone
),

final as (
    select
        customer_id
        , first_name
        , last_name
        , formatted_phone as phone
        , company_id
    
    from format_phone
)

SELECT * FROM final
