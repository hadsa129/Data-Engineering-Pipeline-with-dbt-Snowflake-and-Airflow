select 
* 
from {{ ref('fct_orders') }} 

where  date(order_date) > CURRENT_DATE()
OR  date(order_date) < date('1900-01-01 ')