SELECT  DATE(event_timestamp) as   sales_date,
        product_id,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as units_sold,
        SUM(price) as revenue
        FROM {{ ref('stg_clickstream')}}
        WHERE event_type = 'purchase'
        GROUP BY DATE(event_timestamp), product_id

