{{ config(materialized='table') }}

with sellers as (

    select * from {{ ref('int_seller') }}

)

select * from sellers