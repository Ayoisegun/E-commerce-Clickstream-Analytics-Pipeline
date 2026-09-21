select sale_date, product_id, count(*)
from {{ ref('fct_daily_sales') }}
group by sale_date, product_id
having count(*) > 1