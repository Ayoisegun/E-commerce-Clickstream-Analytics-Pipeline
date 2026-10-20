SELECT  DATE(event_timestamp) as Date,
        COUNT(DISTINCT user_id) as active_users,
        COUNT(DISTINCT session_id) as active_sessions,
        COUNT(event_id) as total_events,
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS no_of_views,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS no_of_purchases,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS no_of_carted
FROM {{ ref ('stg_clickstream') }}
GROUP BY DATE(event_timestamp)