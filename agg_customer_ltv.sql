with per_customer as (
    select
        customer_id,
        min(order_date)               as first_order_date,
        max(order_date)               as last_order_date,
        count(distinct order_id)      as orders,
        sum(net_revenue)              as lifetime_revenue
    from {{ ref('fct_order_items') }}
    where status in ('shipped', 'completed')
    group by customer_id
)
select
    c.customer_id,
    c.full_name,
    c.country,
    p.first_order_date,
    p.last_order_date,
    p.orders,
    p.lifetime_revenue,
    round(p.lifetime_revenue / p.orders, 2) as avg_order_value,
    case
        when p.lifetime_revenue >= 5000 then 'high'
        when p.lifetime_revenue >= 1500 then 'mid'
        else 'low'
    end as value_segment
from per_customer p
inner join {{ ref('dim_customers') }} c
    on p.customer_id = c.customer_id
