select
    f.order_date,
    p.category,
    sum(f.quantity)     as units_sold,
    sum(f.net_revenue)  as net_revenue
from {{ ref('fct_order_items') }} f
inner join {{ ref('dim_products') }} p
    on f.product_id = p.product_id
where f.status in ('shipped', 'completed')
group by f.order_date, p.category
