

WITH transformed_categories as (
  select 
    category_id
    , lower(category_name)
    , description 
  from 
    {{ source('postgres', 'categories') }}
)

SELECT * FROM transformed_categories

