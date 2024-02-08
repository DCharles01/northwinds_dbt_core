{% set columns = ['order_id', 'employee_id', 'customer_id', 'product_id'] %}


with select_orders as (
    
    select  * from {{ source('rds', 'orders') }}
    
),

select_order_detail as (
    select * from {{ source('rds', 'order_details') }}
),

orders_order_detail_join as (
select

    order.order_id as order_id
    , order.employee_id as employee_id
    , order.customer_id as customer_id
    , order_detail.product_id as product_id
    , order.order_date as order_date
    , order.quantity as quantity
    , order.discount as discount
    , order.unit_price as unit_price
from
    {{ source('rds', 'orders') }} order
inner join
    {{ source('rds', 'order_details') }} order_detail on order.order_id=order_detail.order_id

        

),

column_format as (
    select 
    {% for column in columns -%}
        -- {% set column_alias = column.split('.')[1] %}
        {%- if loop.first -%} {{ append_prefix_source(column, 'rds-') }} {%- endif -%} as {{ column }}
        , {{ append_prefix_source(column, 'rds-') }} as {{ column }}
    {%- endfor %}
    , order_date
    , quantity
    , discount
    , unit_price

    from
        orders_order_detail_join
),

final as (
    select * from column_format
)


select * from final

