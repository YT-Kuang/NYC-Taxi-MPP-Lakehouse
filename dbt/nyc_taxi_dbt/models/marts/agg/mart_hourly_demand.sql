select
    pickup_date,
    pickup_hour,
    pickup_borough,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_total_amount
from {{ ref('fact_trip') }}
group by 1, 2, 3