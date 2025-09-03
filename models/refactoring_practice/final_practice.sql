with 

customers as (

    select 
    customer_id,
    surname as customer_last_name,
    givenname as customer_first_name,
    full_name
   from {{ ref('stg_jaffle_shop__customers') }}

),

paid_orders as (

  select * from {{ ref('int_orders_practice') }}

),

final as (

  select
    paid_orders.order_id,
    paid_orders.customer_id,
    paid_orders.order_date,
    paid_orders.order_status,
    paid_orders.total_amount_paid,
    paid_orders.payment_finalized_date,
    customers.customer_first_name,
    customers.customer_last_name,

    -- sales transaction sequence
    row_number() over (order by paid_orders.order_date, paid_orders.order_id) as transaction_seq,

    -- customer sales sequence
    row_number() over (
        partition by paid_orders.customer_id
        order by paid_orders.order_date, paid_orders.order_id
        ) as customer_sales_seq,

    -- new vs returning customer
    case 
      when (
      rank() over (
        partition by paid_orders.customer_id
        order by paid_orders.order_date, paid_orders.order_id
        ) = 1
      ) then 'new'
    else 'return' end as nvsr,

    -- customer lifetime value
    sum(paid_orders.total_amount_paid) over (
      partition by paid_orders.customer_id
      order by paid_orders.order_date, paid_orders.order_id
      ) as customer_lifetime_value,

    -- first day of sale
    first_value(paid_orders.order_date) over (
      partition by paid_orders.customer_id
      order by paid_orders.order_date, paid_orders.order_id
      ) as fdos
    from paid_orders
    left join customers on paid_orders.customer_id = customers.customer_id
)

select 
* 
from final
ORDER BY order_id