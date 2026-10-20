SELECT  product_id,
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS no_of_views,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS no_of_purchases,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS no_of_carted,
        ROUND(
            COUNT(*) FILTER (WHERE event_type = 'add_to_cart')::numeric /
            NULLIF(COUNT(*) FILTER (WHERE event_type = 'product_view'), 0),
            4
        ) AS view_to_cart_rate,
        ROUND(
            COUNT(*) FILTER (WHERE event_type = 'purchase')::numeric /
            NULLIF(COUNT(*) FILTER (WHERE event_type = 'product_view'), 0),
            4
        ) AS view_to_purchase_rate,
        ROUND(
            COUNT(*) FILTER (WHERE event_type = 'purchase')::numeric /
            NULLIF(COUNT(*) FILTER (WHERE event_type = 'add_to_cart'), 0),
            4
        ) AS cart_to_purchase_rate
FROM {{ ref('stg_clickstream') }}
WHERE product_id IS NOT NULL
GROUP BY product_id