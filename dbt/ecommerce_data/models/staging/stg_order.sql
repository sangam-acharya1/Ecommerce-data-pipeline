with source as (

    select * from {{ source('olist_gold', 'orders_enriched') }}

),

renamed as (

    select

        ---------- identifiers ----------
        order_id,
        customer_id,
        seller_id,
        product_id,

        ---------- order info ----------
        order_status,
        total_order_value,
        delivery_days,
        is_late,

        ---------- payment info ----------
        payment_type,
        payment_installments,
        payment_value,

        ---------- product info ----------
        product_category_english        as product_category,
        price                           as product_price,
        freight_value,

        ---------- timestamps ----------
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        ---------- derived date fields ----------
        cast(order_purchase_timestamp as date)          as order_date,
        extract(year  from order_purchase_timestamp)    as order_year,
        extract(month from order_purchase_timestamp)    as order_month,
        extract(dayofweek from order_purchase_timestamp) as order_day_of_week,

        ---------- derived flags ----------
        case
            when order_status = 'delivered' and is_late = false then 'delivered_on_time'
            when order_status = 'delivered' and is_late = true  then 'delivered_late'
            when order_status = 'canceled'                      then 'canceled'
            else order_status
        end                                             as order_status_detailed,

        case
            when total_order_value < 50   then 'low'
            when total_order_value < 200  then 'medium'
            else                               'high'
        end                                             as revenue_band,

        ---------- location ----------
        customer_state,
        seller_state

    from source

    where order_id is not null          -- drop any rows with no order_id

)

select * from renamed