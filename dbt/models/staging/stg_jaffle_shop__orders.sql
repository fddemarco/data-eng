select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from engineering_prod.raw_jaffle_shop.orders
