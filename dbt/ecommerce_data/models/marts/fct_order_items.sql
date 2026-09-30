{{ config(materialized='table') }}

with items as (

    select * from {{ ref('stg_order_items') }}

)

select * from items