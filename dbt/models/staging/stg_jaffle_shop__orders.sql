{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key= 'order_id'
    )
}}

select
    id as order_id,
    user_id as customer_id,
    order_date,
    amount,
    status
from {{ source("jaffle_shop", "orders") }}
{% if is_incremental() %}
where
    dateadd(order_date, 3) >= (select max(order_date) from {{ this }})
{% endif %}
