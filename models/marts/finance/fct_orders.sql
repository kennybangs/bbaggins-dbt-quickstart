with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}
)

select orders.customer_id 
    ,sum (case when payments.status = 'success' then amount end) as lifetime_value 
    from payments 
    left join orders using (order_id)
    group by customer_id
