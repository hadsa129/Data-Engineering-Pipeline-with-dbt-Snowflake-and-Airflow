select 
    order_key,
    sum(item_sale_amount) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from {{ ref('int_orders_items') }}
group by order_key
