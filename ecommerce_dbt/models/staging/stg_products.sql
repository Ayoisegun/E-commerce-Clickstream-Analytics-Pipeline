select
    product_id,
    product_name,
    category,
    brand,
    price
from {{ source('raw', 'products') }}