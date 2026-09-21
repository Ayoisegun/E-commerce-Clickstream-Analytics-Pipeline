SELECT
    session_date AS funnel_date,
    SUM(viewed_product::integer) AS sessions_with_view,
    SUM(added_to_cart::integer) AS sessions_with_cart,
    SUM(purchased::integer) AS sessions_with_purchase,
    ROUND(SUM(added_to_cart::integer) * 1.0 / NULLIF(SUM(viewed_product::integer), 0), 4) AS view_to_cart_rate,
    ROUND(SUM(purchased::integer) * 1.0 / NULLIF(SUM(added_to_cart::integer), 0), 4)AS cart_to_purchase_rate,
    ROUND(SUM(purchased::integer) * 1.0 / NULLIF(SUM(viewed_product::integer), 0), 4) AS view_to_purchase_rate
FROM {{ ref('int_user_sessions') }}
GROUP BY session_date