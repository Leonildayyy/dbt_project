with products as (

    select * from {{ ref('stg_jaffle_shop_products') }}

)

select * from products