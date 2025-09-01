with orders as (
      select 
      order_id,
      customer_id,
      order_date,
      row_number() over ( partition by customer_id order by order_date, order_id ) as user_order_seq,
      status as order_status,
      case 
        when order_status not in ('returned','return_pending') 
        then order_date
      end as valid_order_date
     from {{ ref('stg_orders') }}

),
payments as (
    select   
        payment_id,
        order_id,
        status as payment_status,
        amount as payment_amount
     from {{ ref('stg_payments') }}
    where status != 'fail'
),
order_totals as (
    select
        order_id,
        payment_status,
        sum(payment_amount) as order_value_dollars
    from payments
    group by 1,2
),
order_values_joined as (
    select  
        orders.*,
        order_totals.payment_status,
        order_totals.order_value_dollars
    from orders
    left join order_totals on order_totals.order_id = orders.order_id
)
select * from order_values_joined