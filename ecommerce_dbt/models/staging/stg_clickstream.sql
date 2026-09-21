select
    event_id,
    timestamp as event_timestamp,
    user_id,
    session_id,
    event_type,
    product_id,
    product_name,
    category,
    price
from {{ source('raw', 'raw_clickstream') }}