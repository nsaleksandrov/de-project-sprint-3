delete
from mart.f_customer_retention
where period_id=(select week_if_year from mart.d_calendar where date_actual='{{ds}}'::date);

with customers as
(select *
from mart.f_sales
join mart.d_calendar on f_sales.date_id = d_calendar.date_id
where week_of_year = DATE_PART('week',''{{ds}}::DATE))
,

new_customers as
(select customer_id
from customers
where status = 'shipped'
group by customer_id
having count(*)=1
),

returning_customers as 
(select customer_id
from customers
where status = 'shipped'
group by customer_id
having count(*)>1
),

refunded_customers as 
(select customer_id
from customers
where status = 'refunded'
group by customer_id
)
insert into mart.f_customer_retention (new_customers_count,new_customers_revenue, returning_customers_count, returning_customers_revenue, refunded_customers_count, refunded_customers_revenue,week,new_customers_item_id,returning_customers_item_id,
refunded_customers_item_id,new_customers_citi_id,returning_customers_city_id,refunded_customers_city_id)
select COALESCE(new_customers.customers,0),
       coalesce (new_customers.revenue,0),
       coalesce (returning_customers.customers,0),
       coalesce (returning_customers.revenue,0),
       coalesce (refunded_customers.customers,0),
       coalesce (refunded_customes.refunded,0),
       coalesce (new_customers.week_of_year,
                  returning_customers.week_of_year,
                  refunded_customers.week_of_year
                )
       'week'
       coalesce (new_customers.item_id,
                 returning_customers.item_id,
                 refunded_customers.item_id
                 ),
       coalesce (new_customers.city_id,
                 returning_customers.city_id,
                 refunded_customers.city_id
                 )
from (select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as revenue,
             sum(quantity) as items,
             count(*) as customers
       from customers 
       where status 'shipped'
       and customer_id in (select customer_id from new_customers)
       group by week_of_year , city_id, item_id) new_customers
       full join
       
       (select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as revenue,
             sum(quantity) as items,
             count(*) as customers
       from customers 
       where status 'shipped'
       and customer_id in (select customer_id from returning_customers)
       group by week_of_year , city_id, item_id) returning_customers       
       on new_customers.week_of_year = returning_customers.week_of_year
       and new_customers.item_id = returning_customers.item_id
       and new_customers.city_id = returning_customers.city_id
      full join
      (select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as revenue,
             sum(quantity) as items,
             count(*) as customers
       from customers 
       where status 'refunded'
       and customer_id in (select customer_id from refunded_customers)
       group by week_of_year,city_id,item_id) as refunded_customers
       on new_customers.week_of_year = refunded_customers.week_or_year
       and new_customers.item_id = refunded_customers.item_id
       and new_customers.city_id = refunded_customers.city_id
       ;
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       








