select
    product_id,
    product_name,
    category,
    brand,
    price,
    case
        when price < 50 then 'budget'
        when price < 200 then 'mid'
        else 'premium'
    end as price_band
from {{ ref('stg_products') }}