with source as (

    select * from {{ source('olist_gold', 'seller_metrics') }}

),

renamed as (

    select

        ---------- identifiers ----------
        seller_id,
        seller_state,

        ---------- metrics ----------
        total_orders,
        total_items_sold,
        total_revenue,
        avg_review_score,
        avg_delivery_days,
        late_delivery_rate

    from source

)

select * from renamed