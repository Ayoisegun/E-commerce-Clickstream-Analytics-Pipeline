WITH purchase_revenue AS (
    SELECT
        product_id,
        ROUND(SUM(price)::numeric, 2) AS revenue
    FROM {{ ref('stg_clickstream') }}
    WHERE event_type = 'purchase'
    GROUP BY product_id
)

SELECT
    f.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.price AS catalogue_price,

    f.no_of_views,
    f.no_of_carted AS cart_additions,
    f.no_of_purchases AS purchases,

    f.view_to_cart_rate,
    f.view_to_purchase_rate,
    f.cart_to_purchase_rate,

    COALESCE(r.revenue, 0) AS revenue

FROM {{ ref('int_product_funnel') }} AS f

LEFT JOIN {{ ref('stg_products') }} AS p
    ON f.product_id = p.product_id

LEFT JOIN purchase_revenue AS r
    ON f.product_id = r.product_id