{{ config(materialized='table') }}

with orders as (

    select * from {{ ref('fct_orders') }}

),

aggregated as (

    select

        ---identifier 
        product_category,

        ----volume metrics 
        count(order_id)    as total_orders,
        count(distinct customer_id)   as unique_customers,
        count(distinct seller_id)     as unique_sellers,

        --revenue metrics
        round(sum(total_order_value), 2)   as total_revenue,
        round(avg(total_order_value), 2)   as avg_order_value,
        round(avg(product_price), 2)      as avg_price,
        round(avg(freight_value), 2)       as avg_freight,
        round(sum(freight_value), 2)       as total_freight,

        -- delivery metrics 
        round(avg(delivery_days), 1)         as avg_delivery_days,
        sum(case when delivery_status != 'on_time'
                 then 1 else 0 end)    as late_orders,
        round(  
            sum(case when delivery_status != 'on_time'
                     then 1 else 0 end) * 100.0
            / nullif(count(order_id), 0)
        , 2)      as late_order_pct,

        ---------- revenue bands ----------
        sum(case when revenue_band = 'high'
                 then 1 else 0 end)    as high_band_orders,
        sum(case when revenue_band = 'medium'
                 then 1 else 0 end)    as medium_band_orders,
        sum(case when revenue_band = 'low'
                 then 1 else 0 end)  as low_band_orders,

        ---------- top customer state ----------
        mode() within group (order by customer_state)     as top_customer_state

    from orders
    where product_category is not null
    group by product_category

)

select * from aggregated