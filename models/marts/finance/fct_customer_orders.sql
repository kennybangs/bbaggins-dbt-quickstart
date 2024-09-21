with orders as (
    select *
    from {{ ref('stg_jaffle_shop__orders') }}
),

customers as (
    select *
    from {{ ref('stg_jaffle_shop__customers') }}
),

payments as (
    select *
    from  {{ ref('stg_stripe__payments') }}
),

--latest successful date and total amount per order_id
total_orders as (
    select order_id
    ,status
    --,customer_id
    , max(CREATED) as payment_finalized_date
    , sum(AMOUNT) as total_amount_paid
    from payments
    where STATUS <> 'fail'
    group by 1, 2
),


paid_orders as (
    select orders.order_id as order_id,
    orders.customer_id	as customer_id,
    orders.ORDER_DATE AS order_placed_at,
    orders.STATUS AS order_status,
    total_orders.total_amount_paid,
    total_orders.payment_finalized_date,
    customers.FIRST_NAME    as customer_first_name,
    customers.LAST_NAME as customer_last_name
    FROM orders
    left join total_orders on orders.order_id = total_orders.order_id
    left join customers on orders.customer_id = customers.customer_id
),

customer_orders as (
    select customers.customer_id as customer_id
    , min(ORDER_DATE) as first_order_date
    , max(ORDER_DATE) as most_recent_order_date
    , count(ORDERS.order_id) AS number_of_orders
    from customers
    left join  orders
    on orders.customer_id = customers.customer_id 
    group by 1
    ),

clv as (
    select
    paid_orders.order_id,
    sum(t2.total_amount_paid) as clv_bad
    from paid_orders
    left join paid_orders t2 on paid_orders.customer_id = t2.customer_id and paid_orders.order_id >= t2.order_id
    group by 1
    order by paid_orders.order_id
),

final as (
    select
    paid_orders.*,
    ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,
    CASE WHEN customer_orders.first_order_date = paid_orders.order_placed_at
        THEN 'new'
        ELSE 'return' END as nvsr,
    sum(total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
            ) as customer_lifetime_value,
    customer_orders.first_order_date as first_day_of_sale
    FROM paid_orders
    left join customer_orders USING (customer_id)
    --LEFT OUTER JOIN clv on clv.order_id = paid_orders.order_id
    ORDER BY order_id
)

select *
from final
