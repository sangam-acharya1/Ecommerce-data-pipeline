with source as (

    select * from {{ source('olist_gold', 'order_items_enriched') }}

),

renamed as (

    select
        order_id,
        product_id,
        seller_id,
        customer_id,
        price,
        freight_value,
        item_total_value,
        product_category_english as product_category,
        product_weight_g,
        customer_state,
        seller_state,
        order_status,
        delivery_days,
        is_late

    from source
    where order_id is not null
      and product_id is not null
      and seller_id is not null

)

select * from renamed