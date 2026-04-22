select
    pickup_date,
    payment_type,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_total_amount,
    avg(tip_amount) as avg_tip_amount
from {{ ref('fact_trip') }}
group by 1, 2