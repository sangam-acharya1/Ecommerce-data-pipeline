{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('fct_orders')}}
),
monthly as (
    select 
        order_year,
        order_month, 
        sum(total_order_value) as total_revenue, 
        count(order_id) as total_orders,
        round(avg(total_order_value), 2) as avg_order_value,
        round(sum(freight_value), 2) as total_freight,
        sum(case when is_first_purchase = true then 1 else 0 end) as first_purchases, 
        sum(case when delivery_status = 'on_time' then 1 else 0 end) as on_time_deliveries,

        round(sum(case when revenue_band = 'high' 
                 then total_order_value else 0 end), 2)  as high_band_revenue,
        round(sum(case when revenue_band = 'medium' 
                 then total_order_value else 0 end), 2)  as medium_band_revenue,
        round(sum(case when revenue_band = 'low' 
                 then total_order_value else 0 end), 2)  as low_band_revenue

    from orders
    group by order_year, order_month
),

final as (

    select
        *,

        ---------- previous month revenue ----------
    
        lag(total_revenue) over (
            order by order_year, order_month
        )     as prev_month_revenue,

        ---------- month over month growth % ----------
        round(
            (total_revenue - lag(total_revenue) over (
                order by order_year, order_month)
            ) / nullif(lag(total_revenue) over (
                order by order_year, order_month), 0) * 100
        , 2)       as mom_growth_pct

    from monthly

)
select * from final 