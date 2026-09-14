select
    id as order_id,
    user_id as customer_id,
    order_date,
    amount,
    status as status_description
from {{ source("jaffle_shop", "orders") }}
