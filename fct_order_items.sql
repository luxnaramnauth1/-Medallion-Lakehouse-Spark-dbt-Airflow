-- Grain: one row per order line. Inner join to orders drops orphan lines
-- (orders quarantined in Silver), keeping referential integrity in Gold.
select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    cast(o.order_ts as date)                                         as order_date,
    o.order_ts,
    o.status,
    oi.quantity,
    oi.unit_price,
    oi.discount_pct,
    round(oi.quantity * oi.unit_price * (1 - oi.discount_pct / 100), 2) as net_revenue
from {{ source('silver', 'order_items') }} oi
inner join {{ source('silver', 'orders') }} o
    on oi.order_id = o.order_id
