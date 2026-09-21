select *
from {{ ref('fct_conversion_funnel') }}
where sessions_with_cart > sessions_with_view
   or sessions_with_purchase > sessions_with_cart