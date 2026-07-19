{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('int_customer_segmented') }}
),

final as (
    select 
        -- all original columns
        customer_unique_id,
        total_orders,
        total_spent,
        avg_order_value, 
        first_order_date,
        last_order_date,
        customer_age_days, 
        customer_segment as stg_customer_segment,


        --  customer_segment
    case 
        when total_orders = 1 then 'new'
        when total_orders between 2 and 5 then 'repeated'
        when total_orders > 5 then 'loyal'
        else 'unknown'   
        end as customer_segment,

        --  spending_segment
    case 
        when total_spent < 100 then 'low'
        when total_spent between 100 and 500 then 'medium'
        when total_spent > 500 then 'high'
        else 'unknown'   
        end as spending_segment,


        -- recency_segment
   case
    when datediff('day', last_order_date, current_date) <= 30 then 'active'
    when datediff('day', last_order_date, current_date) between 31 and 90 then 'warm'
    when datediff('day', last_order_date, current_date) between 91 and 180 then 'cooling'
    when datediff('day', last_order_date, current_date) > 180 then 'churned'
    else 'unknown'
end as recency_segment,

        -- customer_value_tier
     case
        when customer_segment = 'loyal' and spending_segment = 'high' and recency_segment = 'active' then 'champion'
        when customer_segment = 'repeated' and spending_segment = 'medium' then 'potential'
        when customer_segment in ('new', 'churned') then 'at_risk'
        else 'unknown'   
        end as customer_value_tier     
    from customers
)

select * from final 