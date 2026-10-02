```sql
with source as (

    select *
    from {{ source('olist_gold', 'order_items_enriched') }}

),

renamed as (

    select

        ---------- identifiers ----------
        order_id,
        order_item_id,
        product_id,
        seller_id,
        customer_id,
        customer_unique_id,

        ---------- financial information ----------
        price,
        freight_value,
        item_total_value,

        ---------- product information ----------
        product_category_english as product_category,
        product_weight_g,

        ---------- location ----------
        customer_state,
        seller_state,

        ---------- order information ----------
        order_status,
        delivery_days,
        is_late

    from source

    where order_id is not null
      and order_item_id is not null
      and product_id is not null
      and seller_id is not null

)

select *
from renamed
```
