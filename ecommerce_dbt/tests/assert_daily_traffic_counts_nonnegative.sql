SELECT *
FROM {{ ref('mart_daily_traffic') }}
WHERE active_users < 0
   OR active_sessions < 0
   OR total_events < 0
   OR no_of_views < 0
   OR no_of_purchases < 0
   OR no_of_carted < 0