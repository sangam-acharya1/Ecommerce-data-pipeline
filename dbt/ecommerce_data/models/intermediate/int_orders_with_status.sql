```sql
with orders as (

    select *
    from {{ ref('stg_order') }}

),

classified as (

    select

        ---------- identifiers ----------
        order_id,
        customer_id,
        customer_unique_id,

        ---------- customer/order info ----------
        order_status,
        order_status_detailed,

        ---------- financial information ----------
        total_order_value,
        total_price,
        freight_value,

        ---------- delivery information ----------
        delivery_days,
        is_late,

        case
            when order_delivered_customer_date is not null
                 and order_estimated_delivery_date is not null
            then greatest(
                datediff(
                    'day',
                    order_estimated_delivery_date,
                    order_delivered_customer_date
                ),
                0
            )
            else null
        end as days_late,

        ---------- payment information ----------
        payment_type,
        payment_installments,
        payment_value,
        payment_installment_records,

        ---------- order composition ----------
        item_count,
        distinct_product_count,
        distinct_seller_count,
        is_multi_seller_order,

        ---------- location ----------
        customer_state,

        ---------- timestamps ----------
        order_date,
        order_year,
        order_month,
        order_day_of_week,
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        ---------- delivery classification ----------
        case
            when order_status = 'canceled'
                then 'not_delivered'

            when order_delivered_customer_date is null
                then 'not_delivered'

            when is_late = false
                then 'on_time'

            when datediff(
                'day',
                order_estimated_delivery_date,
                order_delivered_customer_date
            ) <= 3
                then 'slightly_late'

            when datediff(
                'day',
                order_estimated_delivery_date,
                order_delivered_customer_date
            ) <= 7
                then 'late'

            else 'very_late'

        end as delivery_status,

        ---------- revenue band ----------
        case
            when total_order_value < 50
                then 'low'

            when total_order_value < 200
                then 'medium'

            else 'high'

        end as revenue_band,

        ---------- order day type ----------
        case
            when order_day_of_week in (0, 6)
                then 'weekend'

            else 'weekday'

        end as order_day_type,

        ---------- first purchase flag ----------
        case
            when row_number() over (
                partition by customer_unique_id
                order by order_purchase_timestamp, order_id
            ) = 1
                then true

            else false

        end as is_first_purchase

    from orders

)

select *
from classified
```
