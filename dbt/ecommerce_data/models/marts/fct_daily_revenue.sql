{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('fct_orders')}}
),
aggregated as (
    select 
        order_date, 
        round(sum(total_order_value), 2) as daily_revenue, 
        count(order_id) as total_orders,
        round(avg(total_order_value), 2) as avg_order_value,
        round(sum(freight_value), 2) as total_freight,
        sum(case when is_first_purchase = true then 1 else 0 end) as first_purchases, 
        sum(case when delivery_status = 'on_time' then 1 else 0 end) as on_time_deliveries
    from orders
    group by order_date
)
select * from aggregated