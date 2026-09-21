select *
from {{ ref('stg_clickstream') }}
where event_type = 'purchase'
  and price <= 0