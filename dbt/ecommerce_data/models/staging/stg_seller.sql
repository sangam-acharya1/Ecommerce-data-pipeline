with source as (

    select * from {{ source('olist_gold', 'seller_metrics') }}

),

renamed as (

    select

        ---------- identifiers ----------
        seller_id,

        ---------- metrics ----------
        total_orders,
        total_revenue,
        avg_delivery_days,
        late_delivery_rate


    from source

)

select * from renamed
