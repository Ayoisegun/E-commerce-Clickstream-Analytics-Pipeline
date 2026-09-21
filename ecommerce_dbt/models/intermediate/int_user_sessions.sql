SELECT  session_id,
        user_id,
        min(event_timestamp) as session_start_time,
        max(event_timestamp) as session_end_time,
        max(event_timestamp) - min(event_timestamp) as session_duration,
        DATE(min(event_timestamp)) as session_date,
        BOOL_OR(event_type = 'product_view') as viewed_product,
        BOOL_OR(event_type = 'add_to_cart') as added_to_cart,
        BOOL_OR(event_type = 'purchase') as purchased
FROM {{ ref('stg_clickstream') }}
GROUP BY session_id, user_id