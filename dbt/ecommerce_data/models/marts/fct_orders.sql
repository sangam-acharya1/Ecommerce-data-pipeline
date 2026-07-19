{{ config(materialized='table') }}

with fct_order as (
    select * from {{ ref("int_orders_with_status")}}
),
final as (

    select 
    -- identifiers 
        order_id,
        customer_id, 
        seller_id, 
        product_id, 

    --dates 
        order_date, 
        order_year, 
        order_month, 
        order_day_of_week,
        order_day_type,


     --- financials 
        total_order_value,
        product_price,
        freight_value,
        payment_value,
        payment_type,
        payment_installments,

    -- product
        product_category,

    -- classifications
        order_status_detailed, 
        delivery_status, 
        revenue_band,

    -- flags 
        is_late, 
        is_first_purchase,

    -- delevery metrics 
        delivery_days,

    --  locations 
        customer_state, 
        seller_state

    from fct_order
)

select * from final 