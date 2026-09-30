{{ config(materialized='table') }}

with fct_order as (

    select * from {{ ref('int_orders_with_status') }}

),

final as (

    select
        ---------- identifiers ----------
        order_id,
        customer_id,

        ---------- dates ----------
        order_date,
        order_year,
        order_month,
        order_day_of_week,
        order_day_type,

        ---------- financials ----------
        total_order_value,
        freight_value,
        payment_value,
        payment_type,
        payment_installments,

        ---------- order composition ----------
        item_count,
        distinct_product_count,
        distinct_seller_count,
        is_multi_seller_order,

        ---------- classifications ----------
        order_status_detailed,
        delivery_status,
        revenue_band,

        ---------- flags ----------
        is_late,
        is_first_purchase,

        ---------- delivery metrics ----------
        delivery_days,

        ---------- location ----------
        customer_state

    from fct_order

)

select * from final