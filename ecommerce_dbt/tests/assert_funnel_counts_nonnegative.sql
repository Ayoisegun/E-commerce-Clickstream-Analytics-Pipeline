SELECT *
FROM {{ ref('int_product_funnel') }}
WHERE no_of_views < 0
   OR no_of_purchases < 0
   OR no_of_carted < 0