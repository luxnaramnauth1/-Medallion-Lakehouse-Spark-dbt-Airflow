-- Revenue counts only shipped/completed orders.
select
    order_date,
    count(distinct order_id)                                   as orders,
    sum(quantity)                                              as units_sold,
    sum(net_revenue)                                           as net_revenue,
    round(sum(net_revenue) / count(distinct order_id), 2)      as avg_order_value
from {{ ref('fct_order_items') }}
where status in ('shipped', 'completed')
group by order_date
