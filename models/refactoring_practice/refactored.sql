with

-- Import CTEs

customers as (

  select 
    customer_id,
    surname as customer_last_name,
    givenname as customer_first_name,
    full_name
   from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

  select 
    order_id,
    customer_id,
    order_date,
    order_status,
    valid_order_date
   from {{ ref('stg_jaffle_shop__orders') }}

),
payments as (

  select * from {{ ref('stg_stripe__payments') }}

),
completed_payments as (
    select 
        order_id, 
        max(payment_created_at) as payment_finalized_date, 
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status<> 'fail'
    group by 1
),
paid_orders as (
    select 
        orders.order_id,
        c.customer_id,
        orders.order_date,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.customer_first_name,
        c.customer_last_name
    from orders
    left join completed_payments p on orders.order_id = p.order_id
    left join customers c on orders.customer_id = c.customer_id
),
customer_orders as (
    select 
        customers.customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from customers
    left join orders
    on orders.customer_id = customers.customer_id
    group by 1
),
clv_rolling as (
    select
        p.order_id,
        sum(total_amount_paid) over (
        partition by customer_id
        order by order_date
        rows between unbounded preceding and current row
        )  as clv_bad
    from paid_orders p
    order by p.order_id
)
---final
select
    paid_orders.*,
    ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,
    CASE 
        WHEN c.first_order_date = paid_orders.order_date
        THEN 'new'
        ELSE 'return' 
    END as nvsr,
    cr.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
    FROM paid_orders 
    left join customer_orders c using (customer_id)
    LEFT OUTER JOIN 
       clv_rolling cr on cr.order_id = paid_orders.order_id
ORDER BY order_id
