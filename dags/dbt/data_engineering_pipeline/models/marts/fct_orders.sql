select 
orders.*,
orders_summary.gross_item_sales_amount,
orders_summary.item_discount_amount



from {{ ref('int_order_items_summary') }} as orders_summary join {{ ref('stg_tpch_orders') }}  orders
on orders_summary.order_key = orders.order_key
order by orders.order_date