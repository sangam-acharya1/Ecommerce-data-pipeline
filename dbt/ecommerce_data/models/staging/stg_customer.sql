
with source as (
        select * from {{ source('olist_gold', 'customer_metrics') }}

), 

 renamed as (
    select 
        customer_unique_id, 
        total_orders, 
        total_spent,
        avg_order_value,
        first_order_date,
        last_order_date,
          datediff('day', first_order_date, last_order_date)  as customer_age_days,

        case
            when total_orders = 1       then 'new'
            when total_orders between 2 and 5 then 'repeat'
            else  'loyal'
        end  as customer_segment


        from source
        where customer_unique_id is not null
 )
 select * from renamed

 