select 
    line_item.line_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount') }} as item_discount_amount,
    line_item.extended_price as item_sale_amount
from 
    {{ ref('stg_tpch_orders') }} as orders
join 
    {{ ref('stg_tpch_lineitems') }} as line_item
    on orders.order_key = line_item.order_item_key
order by 
    orders.order_key