select
    order_id,
    sum(amount) as total_amount
from {{ ref('stg_jaffle_shop__orders') }}
group by 1
having total_amount < 0
