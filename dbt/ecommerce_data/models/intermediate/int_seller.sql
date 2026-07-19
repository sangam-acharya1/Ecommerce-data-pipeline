with sellers as (

    select * from {{ ref('stg_seller') }}
    where seller_id is not null

),

ranked as (

    select
        --- identifiers
        
        seller_id,

        -- metrics 
        total_orders,
        total_revenue,
        avg_delivery_days,
        late_delivery_rate,


        --- seller rank by revenue 
        rank() over (
            order by total_revenue desc
        )   as revenue_rank,

        --- delivery reliability 
        case
            when late_delivery_rate = 0     then 'perfect'
            when late_delivery_rate < 10   then 'reliable'
            when late_delivery_rate < 30  then 'unreliable'
            else   'poor'
        end  as delivery_reliability

    from sellers

)

select * from ranked