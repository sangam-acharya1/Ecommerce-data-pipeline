{{ config(materialized='table') }}

with items as (

    select * from {{ ref('fct_order_items') }}

),

aggregated as (

    select

        ---------- identifier ----------
        product_category,

        ---------- volume metrics ----------
        count(distinct order_id)        as total_orders,
        count(*)                        as total_items_sold,
        count(distinct customer_id)     as unique_customers,
        count(distinct seller_id)       as unique_sellers,

        ---------- revenue metrics ----------
        round(sum(item_total_value), 2) as total_revenue,
        round(avg(item_total_value), 2) as avg_item_value,
        round(avg(price), 2)            as avg_price,
        round(avg(freight_value), 2)    as avg_freight,
        round(sum(freight_value), 2)    as total_freight,

        ---------- delivery metrics ----------
        round(avg(delivery_days), 1)    as avg_delivery_days,
        sum(case when is_late then 1 else 0end)                       as late_items,
        round(
            sum(case when is_late then 1 else 0 end) * 100.0
            / nullif(count(*), 0)
        , 2)                             as late_item_pct,

        ---------- item price bands (recomputed at item grain) ----------
        sum(case when price < 50               then 1 else 0 end) as low_band_items,
        sum(case when price between 50 and 200 then 1 else 0 end) as medium_band_items,
        sum(case when price > 200              then 1 else 0 end) as high_band_items,

        ---------- top customer state ----------
        mode() within group (order by customer_state) as top_customer_state

    from items
    where product_category is not null
    group by product_category

)

select * from aggregated