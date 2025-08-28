With payments as
(
    select *
    from {{ ref('stg_payments') }}
    Where status = 'success'
),
pivoted as 
(
    {%- set payment_methods = ["bank_transfer", "coupan", "credit_card", "gift_card"] -%}
    select
        order_id,
        {% for payment_method in payment_methods %}
        sum(case when payment_method = '{{payment_method}}' then amount else 0 end) as {{payment_method}}_amount
            {%- if not loop. last -%} 
               ,
            {%- endif -%} 
        {% endfor %}
    from payments
    group by 1
)
select * from pivoted

