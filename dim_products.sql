select
    product_id,
    name as product_name,
    category,
    unit_price as list_price
from {{ source('silver', 'products') }}
