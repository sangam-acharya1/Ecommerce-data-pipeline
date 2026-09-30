with orders as (

    select * from {{ ref('stg_order') }}

),

classified as (

    select

        ---------- identifiers ----------
        order_id,
        customer_id,

        ---------- original columns ----------
        order_status,
        order_status_detailed,
        total_order_value,
        delivery_days,
        is_late,
        payment_type,
        payment_installments,
        payment_value,
        payment_installment_records,
        freight_value,
        customer_state,
        order_date,
        order_year,
        order_month,
        order_day_of_week,
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        ---------- order composition ----------
        total_price,
        item_count,
        distinct_product_count,
        distinct_seller_count,
        is_multi_seller_order,

        ---------- delivery classification ----------
        case
            when order_status = 'canceled'  then 'not_delivered'
            when delivery_days is null      then 'unknown'
            when is_late = false            then 'on_time'
            when delivery_days <= 3         then 'slightly_late'
            when delivery_days <= 7         then 'late'
            else                                 'very_late'
        end                                             as delivery_status,

        ---------- revenue band ----------
        case
            when total_order_value < 50     then 'low'
            when total_order_value < 200    then 'medium'
            else                                 'high'
        end                                             as revenue_band,

        ---------- order day type ----------
        case
            when order_day_of_week in (1, 7) then 'weekend'
            else                                  'weekday'
        end                                             as order_day_type,

        ---------- first purchase flag ----------
        case
            when rank() over (
                partition by customer_id
                order by order_purchase_timestamp
            ) = 1 then true
            else false
        end                                             as is_first_purchase

    from orders

)

select * from classified